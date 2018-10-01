package io.pravega.leaderelection;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;

@Slf4j
class Client extends Thread implements LeaderElection.LeaderCrashListener {

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
        le.start();
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
    public void selectLeader(String name) {
        log.info("The new leader is: " + name);
    }

    @Override
    public void crashLeader(String name) {
        log.info("The leader: " + name + " is crashed ");
    }


}