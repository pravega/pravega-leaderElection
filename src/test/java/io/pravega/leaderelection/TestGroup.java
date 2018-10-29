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
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;

public class TestGroup {
    private final Map<String, Client> clientMap;
    private final Set<String> connected;
    private static final int ELECTION_TIMEOUT = 5000;

    /**
     * Create a test group with given number of host.
     * @param number the number of the hosts.
     */
    public TestGroup(int number) {
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

    /**
     * Make A new Test group.
     */
    public static TestGroup make_group(int client_number) {
        return new TestGroup(client_number);
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
     * check there's exactly one leader.
     * try a few times in case re-elections are needed.
     */
    public String checkOneLeader() throws InterruptedException {

        HashSet<String> leaders = new HashSet<>();
        for (int i =0; i < 10; i++) {
            Thread.sleep(ELECTION_TIMEOUT);
            for (String host: clientMap.keySet()) {
                if (connected.contains(host)) {
                    Client t = clientMap.get(host);
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
     * return all connected hosts.
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


    public boolean checkConnectHost() {
        Set<String> res = new HashSet<>(getAllConnectHost());

        for (String host: clientMap.keySet()) {
            if (connected.contains(host)) {
                Set<String> members = clientMap.get(host).getAllMembers();
                if(!res.equals(members)) {
                    return false;
                }
            }
        }
        return true;
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
