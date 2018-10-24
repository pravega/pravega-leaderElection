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
import org.junit.Assert;
import org.junit.Test;
import java.util.List;


public class LeaderElectionTest {
    @Test
    public void testInitialElection3() throws InterruptedException {
        int host = 3;
        TestGroup cfg = TestGroup.make_group(host);
        String newLeader = cfg.checkOneLeader();
        Assert.assertNotNull(newLeader);
        cfg.stop();
    }

    @Test
    public void testMemberCrash3() throws InterruptedException {
        int host = 3;
        TestGroup cfg = TestGroup.make_group(host);
        String leader = cfg.checkOneLeader();
        Assert.assertNotNull(leader);

        cfg.remain(leader);
        String leader2 = cfg.checkOneLeader();
        Assert.assertNotNull(leader2);
        Assert.assertEquals(leader2, leader);

        cfg.restartExcept(leader2);
        String leader3 = cfg.checkOneLeader();
        Assert.assertNotNull(leader3);
        Assert.assertEquals(leader3, leader2);

        cfg.stop();
    }

    @Test
    public void testReElection3() throws InterruptedException {
        int host = 2;
        TestGroup cfg = TestGroup.make_group(host);
        String leader = cfg.checkOneLeader();
        Assert.assertNotNull(leader);

        cfg.disconnect(leader);
        String leader2 = cfg.checkOneLeader();
        Assert.assertNotNull(leader2);

        cfg.connect(leader);
        String leader3 = cfg.checkOneLeader();
        Assert.assertNotNull(leader3);
        Assert.assertEquals(leader3, leader2);

        cfg.stop();
    }

    @Test
    public void testLeaderCorrectness3() throws InterruptedException {
        int host = 3;
        TestGroup cfg = TestGroup.make_group(host);
        String leader = cfg.checkOneLeader();
        Assert.assertNotNull(leader);

        List<String> hosts = cfg.getAllConnectHost();
        hosts.remove(leader);
        String h1 = hosts.get(0);
        String h2 = hosts.get(1);


        cfg.disconnect(h2);
        Thread.sleep(1000);
        cfg.connect(h2);


        cfg.disconnect(leader);
        String newLeader = cfg.checkOneLeader();
        Assert.assertNotNull(newLeader);
        Assert.assertEquals(newLeader, h1);
        cfg.stop();
    }

    @Test
    public void testAllCrash3() throws InterruptedException {
        int host = 3;
        TestGroup cfg = TestGroup.make_group(host);
        String leader = cfg.checkOneLeader();
        Assert.assertNotNull(leader);

        cfg.disconnect(leader);
        String newLeader = cfg.checkOneLeader();
        Assert.assertNotNull(newLeader);

        cfg.disconnect(newLeader);
        newLeader = cfg.checkOneLeader();
        Assert.assertNotNull(newLeader);


        cfg.disconnect(newLeader);
        newLeader = cfg.checkOneLeader();
        Assert.assertNull(newLeader);

        cfg.stop();
    }

    @Test
    public void testReElection5() throws InterruptedException {
        int host = 5;

        TestGroup cfg = TestGroup.make_group(host);
        String leader = cfg.checkOneLeader();
        Assert.assertNotNull(leader);

        cfg.disconnect(leader);
        List<String> hosts = cfg.getAllConnectHost();
        //disconnect one host
        cfg.disconnect(hosts.get(0));
        cfg.disconnect(hosts.get(1));
        cfg.disconnect(hosts.get(2));
        Thread.sleep(1000);
        cfg.connect(hosts.get(1));
        cfg.connect(hosts.get(2));
        cfg.connect(hosts.get(0));
        String newLeader = cfg.checkOneLeader();
        Assert.assertNotNull(newLeader);
        Assert.assertEquals(newLeader, hosts.get(3));

        cfg.disconnect(newLeader);
        cfg.disconnect(hosts.get(1));
        cfg.disconnect(hosts.get(2));
        Thread.sleep(1000);
        cfg.connect(hosts.get(1));
        cfg.connect(hosts.get(2));
        newLeader = cfg.checkOneLeader();
        Assert.assertNotNull(newLeader);
        Assert.assertEquals(newLeader, hosts.get(0));

        cfg.stop();
    }

}
