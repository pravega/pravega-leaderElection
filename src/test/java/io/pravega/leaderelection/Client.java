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
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
/**
 * A client thread that acts as a member in leader election.
 */
@Slf4j
class Client extends Thread implements LeaderElection.LeaderElectionCallback {

    private static final String DEFAULT_SCOPE = "election";
    private static final String DEFAULT_CONFIG_NAME = "leaderElection";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    private final LeaderElection le;

    /**
     * Create a client with the given name,
     * using default scopeName, streamName and uri to create leader election object.
     * @param hostName  the name
     */
    public Client(String hostName) {
        URI controllerURI = URI.create(DEFAULT_CONTROLLER_URI);
        le = new LeaderElection(DEFAULT_SCOPE, DEFAULT_CONFIG_NAME, controllerURI,hostName, this);
    }

    /**
     * Start o sending heartbeat at the speed of the 500ms.
     */
    @Override
    public void run() {
        le.start(500);
    }

    /**
     * Stop sending heartbeat to pravega state synchronizer.
     */
    public void stopRunning() {
        le.stop();
    }

    /**
     * Get the current instance name.
     * @return the name of the instance.
     */
    public String get() {
        return le.getInstanceId();
    }

    /**
     * Get the current leader of the group.
     * @return the name of the leader.
     */
    public String getLeader() {
        return le.getCurrentLeader();
    }


    @Override
    public void onNewLeader(String name) {
        log.info(name + " become leader!");
    }

    @Override
    public void startActingLeader() {
        log.info(le.getInstanceId() + "start acting leader");
    }

    @Override
    public void stopActingLeader() {
        log.info(le.getInstanceId() + "stop acting leader");
    }
}
