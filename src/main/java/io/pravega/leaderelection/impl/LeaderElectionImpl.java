/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.leaderelection.impl;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import java.io.Serializable;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.Update;
import io.pravega.client.state.InitialUpdate;
import io.pravega.leaderelection.LeaderElection.LeaderElectionCallback;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class LeaderElectionImpl extends AbstractService{

    /**
     * Number of intervals behind before another host should be considered dead.
     */
    private static final int DEATH_THRESHOLD = 10;
    /**
     * Number of intervals behind before we should stop executing for safety.
     */
    private static final int UNHEALTHY_THRESHOLD = 3;

    /**
     * The  Universally Unique Identifier to identify LeaderElection Synchronizer.
     */
    private final String instanceId;

    private final AtomicBoolean healthy = new AtomicBoolean();

    private String leaderName = null;

    /**
     *  The initial timeout cycle for each host.
     */
    private static final double INITIAL_TIMEOUT = 1.0;
    /**
     * The heartbeat rate in pre
     */
    private int updateRate;
    /**
     * The local state of the shared Membership state.
     */
    private final StateSynchronizer<LiveInstances> stateSync;
    /**
     * The executor Service that schedule commands to run periodically.
     */
    private final ScheduledExecutorService executor;
    /**
     * The callback function.
     */
    private final LeaderElectionCallback listener;
    /**
     * The result of the scheduling executor.
     */
    private ScheduledFuture<?> task;

    /**
     * Create a leaderElection using a synchronizer based on the given stream name.
     * @param scope the Scope to use to create the Stream used by the StateSynchronizer.
     * @param name the name of the Stream to be used by the StateSynchronizer.
     * @param clientFactory the clientFactory to use to create the StateSynchronizer.
     * @param streamManager the pravega StreamManager to create the Stream used by the StateSynchronizer.
     * @param executor the scheduled executor to be used for heartbeater.
     */
    public LeaderElectionImpl(String scope, String name, ClientFactory clientFactory, StreamManager streamManager,
                       ScheduledExecutorService executor, LeaderElectionCallback listener, String hostName) {

        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(clientFactory);
        Preconditions.checkNotNull(streamManager);
        Preconditions.checkNotNull(listener);
        Preconditions.checkNotNull(hostName);
        this.executor =  executor;
        this.listener = listener;
        this.instanceId = hostName;

        streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder().
                scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(scope, name,streamConfig);

        stateSync = clientFactory.createStateSynchronizer(name,
                                                    new JavaSerializer<HeartbeatUpdate>(),
                                                    new JavaSerializer<CreateState>(),
                                             SynchronizerConfig.builder().build());

        stateSync.initialize(new CreateState(new HashMap<>()));
    }

    @Data
    private static class Instance implements Serializable {
        private final long timestamp;
        private final long times;
        private final double timeout;

        private static int compare(Instance o1, Instance o2) {
            return Long.compare(o1.timestamp, o2.timestamp);
        }

    }

    @Data
    private static class LiveInstances implements Revisioned, Comparable<LiveInstances>, Serializable {

        private static final long serialVersionID = 1L;
        private final String scopedStreamName;
        private final Revision revision;
        private final Map<String, Instance> liveInstances;
        /**
         * The vectorTime always record the up to date time in
         */
        private final long vectorTime;
        /**
         * The number of the leader, default value is null.
         */
        private final String leaderName;

        @Override
        public int compareTo(LiveInstances o) {
            return revision.compareTo(o.revision);
        }

        private static int compare(Entry<String, Instance> o1 , Entry<String, Instance> o2) {
            return Long.compare(o1.getValue().times, o2.getValue().times);
        }

        /**
         * Return all instances are dead at given time.
         * @param vectorTime The given time.
         * @return A list of the instances that are dead.
         */
        private List<String> findInstancesThatWillDieBy(long vectorTime) {
            double deathThreshold;
            double deathCycle;
            List<String> res = new ArrayList<>();
            for (String key: liveInstances.keySet()) {
                deathCycle = DEATH_THRESHOLD * liveInstances.get(key).timeout;
                deathThreshold = (deathCycle + 1) * (liveInstances.size() - 1);
                if (liveInstances.get(key).timestamp < (vectorTime - deathThreshold)) {
                    res.add(key);
                }
            }
            return res;
        }

        public boolean isHealthy(String name) {
            long unhealthyThreshold = vectorTime - UNHEALTHY_THRESHOLD * liveInstances.size();
            Instance instance = liveInstances.get(name);
            return instance == null || instance.timestamp >= unhealthyThreshold;
        }
        /**
         * Return all instances that are alived.
         * @return A set of alive instances.
         */
        private Set<String> getLiveInstances() {
            return liveInstances.keySet();
        }

    }

    @RequiredArgsConstructor
    private static class CreateState implements Serializable, InitialUpdate<LiveInstances> {
        private static final long serialVersionID = 1L;
        private final Map<String, Instance> liveInstances;

        @Override
        public LiveInstances create(String scopedStreamName, Revision revision) {
            return new LiveInstances(scopedStreamName, revision, liveInstances, 0, null);
        }
    }

    private static abstract class HeartbeatUpdate implements Update<LiveInstances>, Serializable {
        private static final long serialVersionUID = 1L;
    }

    @RequiredArgsConstructor
    private static class HeartBeat extends HeartbeatUpdate {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final Instance temp;

        @Override
        public LiveInstances applyTo(LiveInstances state, Revision newRevision) {
            Map<String, Instance> tempInstances = new HashMap<>(state.liveInstances);
            long vectorTime = Long.max(tempInstances.values().stream().max(Instance::compare).get().timestamp, temp.timestamp);

            tempInstances.put(name, temp);
            return new LiveInstances(state.scopedStreamName,
                    newRevision,
                    Collections.unmodifiableMap(tempInstances),
                    vectorTime,
                    state.leaderName);
        }
    }

    @RequiredArgsConstructor
    private static class DeclareDead extends HeartbeatUpdate {
        private static final long serialVersionUID = 1L;
        private final String name;

        @Override
        public LiveInstances applyTo(LiveInstances state, Revision newRevision) {
            Map<String, Instance> tempInstances = new HashMap<>(state.liveInstances);
            tempInstances.remove(name);
            return new LiveInstances(state.scopedStreamName,
                    newRevision,
                    Collections.unmodifiableMap(tempInstances),
                    state.vectorTime,
                    state.leaderName);
        }
    }

    @RequiredArgsConstructor
    private static class LeaderSet extends HeartbeatUpdate {
        private static final long serialVersionUID = 1L;
        private final String newLeader;
        @Override
        public LiveInstances applyTo(LiveInstances state, Revision newRevision) {
                Map<String, Instance> tempInstances = new HashMap<>(state.liveInstances);

                for(String key: tempInstances.keySet()) {
                    Instance temp = tempInstances.get(key);
                    tempInstances.put(key, new Instance(temp.timestamp, 1, temp.timeout));
                }

                return new LiveInstances(state.scopedStreamName,
                        newRevision,
                        Collections.unmodifiableMap(state.liveInstances),
                        state.vectorTime,
                        newLeader);
        }
    }

    @RequiredArgsConstructor
    private static class AddMember extends HeartbeatUpdate {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final long time;

        @Override
        public LiveInstances applyTo(LiveInstances state, Revision newRevision) {
            Map<String, Instance> tempInstances = new HashMap<>(state.liveInstances);
            tempInstances.put(name, new Instance(time, 1, INITIAL_TIMEOUT));
            return new LiveInstances(state.scopedStreamName,
                    newRevision,
                    Collections.unmodifiableMap(tempInstances),
                    state.vectorTime,
                    state.leaderName);
        }
    }


    private class HeartBeater implements Runnable {
        @Override
        public void run() {
            try {
                stateSync.fetchUpdates();
                notifyListener();
                stateSync.updateState((state, updates) -> {
                    long vectorTime = state.getVectorTime() + 1;
                    Instance temp = state.liveInstances.get(instanceId);
                    long newTimes = temp.times + 1;
                    double newTimeout = (temp.timeout * temp.times +
                            (vectorTime - temp.timestamp) * 1.0 /
                                    state.liveInstances.size()) / newTimes;

                    updates.add(new HeartBeat(instanceId, new Instance(vectorTime, newTimes, newTimeout)));

                    for (String id : state.findInstancesThatWillDieBy(vectorTime)) {
                        if (!id.equals(instanceId)) {
                            updates.add(new DeclareDead(id));
                            if (id.equals(state.leaderName)) {
                                String newLeader = state.liveInstances.entrySet()
                                        .stream().max(LiveInstances::compare).get().getKey();
                                updates.add(new LeaderSet(newLeader));
                            }
                        }
                    }
                    // for initial state or other states that leader doesn't exist
                    if (state.leaderName == null) {
                        String newLeader = state.liveInstances.entrySet()
                                .stream().max(LiveInstances::compare).get().getKey();
                        updates.add(new LeaderSet(newLeader));
                    }
                });

                // when leader changes, notify to all.
                if (leaderName == null || !leaderName.equals(stateSync.getState().leaderName)) {
                    leaderName = stateSync.getState().leaderName;
                    listener.onNewLeader(leaderName);
                }
                // check host healthy
                notifyListener();

            } catch (Exception e) {
                log.warn("Encountered an error while heartbeating: " + e);
                if (healthy.compareAndSet(true,false) && instanceId.equals(leaderName)) {
                    listener.stopActingLeader();
                }
            }
        }
    }

    /**
     * Add a new host into the group.
     */
    public void add() {
        stateSync.updateState((state, updates) -> {
            updates.add(new AddMember(instanceId, state.getVectorTime()));
        });
    }


    public void notifyListener() {
        LiveInstances currentState = stateSync.getState();
        if (currentState.isHealthy(instanceId)) {
            if (healthy.compareAndSet(false, true)) {
                if (instanceId.equals(leaderName)) {
                    listener.startActingLeader();
                }
            }
        } else {
            if (healthy.compareAndSet(true, false)) {
                if (instanceId.equals(leaderName)) {
                    listener.stopActingLeader();
                }
            }
        }
    }

    public Set<String> getCurrentMembers() {
        return stateSync.getState().getLiveInstances();
    }

    public String getCurrentLeader() {
        return leaderName;
    }

    public String getInstanceId() {
        return instanceId;
    }
    
    public void setRate(int updateRate) {
        this.updateRate = updateRate;
    }

    public void close() {
        if (stateSync != null) {
            stateSync.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Override
    protected void doStart() {
        task = executor.scheduleAtFixedRate(new HeartBeater(),
                                            updateRate,
                                            updateRate,
                                            TimeUnit.MILLISECONDS);
        notifyStarted();
    }

    @Override
    protected void doStop() {
        task.cancel(false);
        executor.shutdown();
        notifyStopped();
    }

}
