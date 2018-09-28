package io.pravega.leaderelection;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;

class Client extends Thread {

    private static final String DEFAULT_SCOPE = "test";
    private static final String DEFAULT_CONFIG_NAME = "leaderElection";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://127.0.0.1:9090";
    private URI controllerURI;
    private LeaderElection.Callback callback;
    private LeaderElection le;
    private volatile boolean flag;


    public Client(String hostName) {
        controllerURI = URI.create(DEFAULT_CONTROLLER_URI);
        callback = new Callback_Instance();
        le = new LeaderElection(DEFAULT_SCOPE, DEFAULT_CONFIG_NAME, controllerURI, callback, hostName);
        flag = true;
    }

    @Override
    public void run() {
        le.create();
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


    @Slf4j
    private static class Callback_Instance implements LeaderElection.Callback {

        @Override
        public void selectLeader(String name) {
            log.info("The new leader is: " + name);
        }

        @Override
        public void crashLeader(String name) {
            log.info("The leader: " + name + " is crashed ");
        }
    }

}