package io.pravega.leaderelection;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

public class TestConfig {
    private Map<String, Client> clientMap;
    private Set<String> connected;
    private static final int ELECTION_TIMEOUT = 5000;

    public TestConfig(int number) {
        clientMap = new HashMap<>();
        connected = new HashSet<>();
        createClients(number);

    }

    private void createClients(int number) {
        for (int i = 0; i < number; i++) {
            Client temp = new Client("Host-" + i);
            clientMap.put(temp.get(), temp);
            connected.add(temp.get());
            temp.start();
        }
    }

    public static TestConfig make_config(int client_number) {
        return new TestConfig(client_number);
    }

    /**
     * detach server host from the membership.
     */
    public void disconnect(String host) {
        Client toStop = clientMap.get(host);
        connected.remove(host);
        toStop.stopRunning();
    }

    /**
     * re-join the server host into the membership.
     */
    public void connect(String host) {
        Client toStart = new Client(host);
        clientMap.put(host,toStart);
        connected.add(host);
        toStart.start();
    }

    /**
     * detach all servers except the leader/
     */
    public void remain(String leader) {
        for (String host: clientMap.keySet()) {
            if (!host.equals(leader)) {
                clientMap.get(host).stopRunning();
                connected.remove(host);
            }
        }
    }

    /**
     * restart all servers except the leader.
     */
    public void restartExcept(String leader) {
        for (String host: clientMap.keySet()) {
            if (!host.equals(leader)) {
                Client toStart = new Client(host);
                toStart.start();
                clientMap.put(host, toStart);
                connected.add(host);
            }
        }
    }

    /**
     * check that there's exactly one leader.
     * try a few times in case re-elections are needed.
     */
    public String checkOneLeader() throws InterruptedException {

        HashSet<String> leaders = new HashSet<>();
        for (int i =0; i < 10; i++) {
            Thread.sleep(ELECTION_TIMEOUT);
            for (String host: clientMap.keySet()) {
                if (connected.contains(host)) {
                    Client t = clientMap.get(host);
                    System.out.println(t.get());
                    leaders.add(t.getLeader());
                }
            }
            if (leaders.size() > 1 || leaders.contains(null)) {
                return null;
            }
        }

        if (leaders.iterator().hasNext()) {
            return leaders.iterator().next();
        }
        return null;
    }

    /**
     * check that there's no leader.
     */
    public boolean checkNoLeader() {
        for (String host: clientMap.keySet()) {
            if (connected.contains(host)) {
                Client t = clientMap.get(host);
                String leader = t.getLeader();
                if (leader != null) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * return all connected host.
     */
    public List<String> getAllConnectHost() {
        List<String> res = new ArrayList<>();
        for (String host: clientMap.keySet()) {
            if (connected.contains(host)) {
                res.add(host);
            }
        }
        return res;
    }

    /**
     * stop all the clients.
     */
    public void stop() {
        for (Client t: clientMap.values()) {
            t.stopRunning();
        }
    }
}
