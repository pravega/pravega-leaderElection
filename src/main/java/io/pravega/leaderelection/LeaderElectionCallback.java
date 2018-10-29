package io.pravega.leaderelection;

/**
 * The interface of LeaderElection callback.
 */
public interface LeaderElectionCallback {
    /**
     * When leader changes, every host will call this function.
     * @param name The host which become leader.
     */
    void onNewLeader(String name);

    /**
     * When leader is unhealthy and become healthy,
     * it will call this function to act as leader.
     */
    void startActingLeader();

    /**
     * When leader is healthy and become unhealthy,
     * it will call this function to stop acting as leader.
     */
    void stopActingLeader();
}
