package io.pravega.leaderelection;

public class Leader {
    private String leaderName;
    private Leader() {
        leaderName = null;
    }

    private static class LeaderSingleton {
        private static final Leader INSTANCE = new Leader();
    }

    public static Leader getInstance() {
        return LeaderSingleton.INSTANCE;
    }

    public String getLeader() {
        return leaderName;
    }

    public String setLeader(String name) {
        leaderName = name;
    }

    public Boolean isLeader(String name) {
        return leaderName.equals(name);
    }
}
