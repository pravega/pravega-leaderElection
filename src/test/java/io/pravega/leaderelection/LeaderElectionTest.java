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
import java.util.Random;


public class LeaderElectionTest {
    /**
     * Test if there is a leader when there are 3 hosts.
     */
    @Test
    public void testInitialElection3() throws InterruptedException {
        int host = 3;
        TestGroup cfg = TestGroup.make_group(host);
        cfg.startRun();
        String newLeader = cfg.checkOneLeader();
        Assert.assertNotNull(newLeader);
        cfg.stop();
    }

    /**
     * Test when all follower crash and re-join,
     * it will not change the leader.
     */
    @Test
    public void testMemberCrash3() throws InterruptedException {
        int host = 3;
        TestGroup cfg = TestGroup.make_group(host);
        cfg.startRun();
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

    /**
     * Test when leader crashed, it will select a new leader.
     * When old leader comes back, it will not affect new leader.
     */
    @Test
    public void testReElection3() throws InterruptedException {
        int host = 3;
        TestGroup cfg = TestGroup.make_group(host);
        cfg.startRun();
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

    /**
     * Test the correctness of selecting new leader.
     * it will make some hosts detach for some time then come back.
     * Make sure that select most healthy host as new leader.
     */
    @Test
    public void testLeaderCorrectness3() throws InterruptedException {
        int host = 3;
        TestGroup cfg = TestGroup.make_group(host);
        cfg.startRunWithDelay();
        String leader = cfg.checkOneLeader();
        Assert.assertNotNull(leader);

        cfg.disconnect(leader);
        String newLeader = cfg.checkOneLeader();
        Assert.assertNotNull(newLeader);
        Assert.assertEquals(newLeader, "Host-1");
        cfg.stop();
    }

    /**
     * Test leader crashed one by one.
     */
    @Test
    public void testAllCrash3() throws InterruptedException {
        int host = 3;
        TestGroup cfg = TestGroup.make_group(host);
        cfg.startRun();
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

    /**
     * Test Re-election with 5 hosts.
     */
    @Test
    public void testReElection5() throws InterruptedException {
        int host = 5;
        TestGroup cfg = TestGroup.make_group(host);
        cfg.startRunWithDelay();
        String leader = cfg.checkOneLeader();
        Assert.assertNotNull(leader);
        cfg.disconnect(leader);
        String newLeader = cfg.checkOneLeader();
        Assert.assertNotNull(newLeader);
        Assert.assertEquals(newLeader, "Host-1");
        cfg.disconnect(newLeader);
        newLeader = cfg.checkOneLeader();
        Assert.assertNotNull(newLeader);
        Assert.assertEquals(newLeader, "Host-2");
        cfg.stop();
    }

    /**
     * Test random crash of the member then check there is a leader
     * and only the live instances.
     */
    @Test
    public void testRandomCrash() throws InterruptedException {
        int host = 5;
        Random random = new Random();
        TestGroup cfg = TestGroup.make_group(host);
        cfg.startRun();
        String leader = cfg.checkOneLeader();
        Assert.assertNotNull(leader);
        // random disconnect one member
        for (int i = 0; i < 10; i++) {
            int number = random.nextInt(5);
            cfg.disconnect("Host-" + number);
            leader = cfg.checkOneLeader();
            Assert.assertNotNull(leader);
            Assert.assertTrue(cfg.checkConnectHost());
            cfg.connect("Host-" + number);
        }
    }

}
