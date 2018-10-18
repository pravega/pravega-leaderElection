/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.leaderelection;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;

@Slf4j
class Client extends Thread implements LeaderElection.LeaderElectionCallback {

    private static final String DEFAULT_SCOPE = "election";
    private static final String DEFAULT_CONFIG_NAME = "leaderElection";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    private URI controllerURI;
    private LeaderElection le;
    private volatile boolean flag;


    public Client(String hostName) {
        controllerURI = URI.create(DEFAULT_CONTROLLER_URI);
        le = new LeaderElection(DEFAULT_SCOPE, DEFAULT_CONFIG_NAME, controllerURI,hostName, this);
        flag = true;
    }

    @Override
    public void run() {
        le.start(500);
        while(flag);
        le.stop();
        le.close();
    }

    public void stopRunning() {
        flag = false;
    }

    public String get() {
        return le.getInstanceId();
    }

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