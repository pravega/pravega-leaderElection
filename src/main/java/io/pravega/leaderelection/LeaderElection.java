/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.leaderelection;
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
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class LeaderElection extends AbstractService{

    /**
     * Number of intervals behind before another host should be considered dead.
     */
    private static final int DEATH_THRESHOLD = 6;
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

    private final ClientFactory clientFactory;
    private final StreamManager streamManager;

    public LeaderElection(String scopeName, String streamName, URI controllerURI, String hostName,
                          LeaderElectionCallback listener) {

        Preconditions.checkNotNull(scopeName);
        Preconditions.checkNotNull(streamName);
        Preconditions.checkNotNull(controllerURI);
        Preconditions.checkNotNull(listener);
        Preconditions.checkNotNull(hostName);
        this.listener = listener;
        this.instanceId = hostName;
        this.clientFactory = ClientFactory.withScope(scopeName, controllerURI);
        this.streamManager = StreamManager.create(controllerURI);
        this.executor = Executors.newScheduledThreadPool(1);
        streamManager.createScope(scopeName);
        StreamConfiguration streamConfig = StreamConfiguration.builder().
                scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(scopeName, streamName,streamConfig);

        stateSync = clientFactory.createStateSynchronizer(streamName,
                                                    new JavaSerializer<HeartbeatUpdate>(),
                                                    new JavaSerializer<CreateState>(),
                                             SynchronizerConfig.builder().build());

        stateSync.initialize(new CreateState(new HashMap<>()));
    }

    @Data
    private static class InstanceInfo implements Serializable {
        private final long timestamp;
        private final long times;
        private final double timeout;

        private static int compare(InstanceInfo o1, InstanceInfo o2) {
            return Long.compare(o1.timestamp, o2.timestamp);
        }

    }

    @Data
    private static class LiveInstances implements Revisioned, Comparable<LiveInstances>, Serializable {

        private static final long serialVersionID = 1L;
        private final String scopedStreamName;
        private final Revision revision;
        private final Map<String, InstanceInfo> liveInstances;
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

        private static int compare(Entry<String, InstanceInfo> o1 , Entry<String, InstanceInfo> o2) {
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
            InstanceInfo instanceinfo = liveInstances.get(name);
            return instanceinfo == null || instanceinfo.timestamp >= unhealthyThreshold;
        }
        /**
         * Return all instances that are alived.
         * @return A set of alive instances.
         */
        private Set<String> getLiveInstances() {
            return Collections.unmodifiableSet(liveInstances.keySet());
        }

    }

    @RequiredArgsConstructor
    private static class CreateState implements Serializable, InitialUpdate<LiveInstances> {
        private static final long serialVersionID = 1L;
        private final Map<String, InstanceInfo> liveInstances;

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
        private final InstanceInfo instanceInfo;

        @Override
        public LiveInstances applyTo(LiveInstances state, Revision newRevision) {
            Map<String, InstanceInfo> tempInstances = new HashMap<>(state.liveInstances);
            long vectorTime = Long.max(tempInstances.values().stream().max(InstanceInfo::compare).get().timestamp, instanceInfo.timestamp);

            tempInstances.put(name, instanceInfo);
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
            Map<String, InstanceInfo> tempInstances = new HashMap<>(state.liveInstances);
            tempInstances.remove(name);
            return new LiveInstances(state.scopedStreamName,
                    newRevision,
                    Collections.unmodifiableMap(tempInstances),
                    state.vectorTime,
                    state.leaderName);
        }
    }

    @RequiredArgsConstructor
    private static class SetLeader extends HeartbeatUpdate {
        private static final long serialVersionUID = 1L;
        private final String newLeader;
        @Override
        public LiveInstances applyTo(LiveInstances state, Revision newRevision) {
                Map<String, InstanceInfo> tempInstances = new HashMap<>(state.liveInstances);

                for(String key: tempInstances.keySet()) {
                    InstanceInfo info = tempInstances.get(key);
                    tempInstances.put(key, new InstanceInfo(info.timestamp, 1, info.timeout));
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
            Map<String, InstanceInfo> tempInstances = new HashMap<>(state.liveInstances);
            tempInstances.put(name, new InstanceInfo(time, 1, INITIAL_TIMEOUT));
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
                    InstanceInfo instanceInfo = state.liveInstances.get(instanceId);
                    long newTimes = instanceInfo.times + 1;
                    double newTimeout = (instanceInfo.timeout * instanceInfo.times +
                            (vectorTime - instanceInfo.timestamp) * 1.0 /
                                    state.liveInstances.size()) / newTimes;

                    updates.add(new HeartBeat(instanceId, new InstanceInfo(vectorTime, newTimes, newTimeout)));

                    for (String id : state.findInstancesThatWillDieBy(vectorTime)) {
                        if (!id.equals(instanceId)) {
                            updates.add(new DeclareDead(id));
                            if (id.equals(state.leaderName)) {
                                String newLeader = state.liveInstances.entrySet()
                                        .stream().max(LiveInstances::compare).get().getKey();
                                updates.add(new SetLeader(newLeader));
                            }
                        }
                    }
                    // for initial state or other states that leader doesn't exist
                    if (state.leaderName == null) {
                        String newLeader = state.liveInstances.entrySet()
                                .stream().max(LiveInstances::compare).get().getKey();
                        updates.add(new SetLeader(newLeader));
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

    private void setRate(int updateRate) {
        this.updateRate = updateRate;
    }

    private void add() {
        if (!stateSync.getState().liveInstances.containsKey(instanceId)) {
            stateSync.updateState((state, updates) -> {
                updates.add(new AddMember(instanceId, state.getVectorTime()));
            });
        }
    }

    public void start(int updateRate){
        setRate(updateRate);
        add();
        startAsync();
    }

    public void stop() {
        stopAsync();
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

    public interface LeaderElectionCallback {
        void onNewLeader(String name);
        void startActingLeader();
        void stopActingLeader();
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


    public void close() {
        if (stateSync != null) {
            stateSync.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        if (streamManager != null) {
            streamManager.close();
        }
        if (clientFactory != null) {
            clientFactory.close();
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
