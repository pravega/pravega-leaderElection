package io.pravega.leaderelection;
import junit.framework.TestCase;
import org.junit.Test;
import java.util.List;


public class LeaderElectionTest extends TestCase {
    @Test
    public void testInitialElection3() throws InterruptedException {
        int host = 3;
        TestConfig cfg = TestConfig.make_config(host);
        String newLeader = cfg.checkOneLeader();
        assertNotNull(newLeader);
        cfg.stop();
    }

    @Test
    public void testMemberCrash3() throws InterruptedException {
        int host = 3;
        TestConfig cfg = TestConfig.make_config(host);
        String leader = cfg.checkOneLeader();
        assertNotNull(leader);

        cfg.remain(leader);
        String leader2 = cfg.checkOneLeader();
        assertNotNull(leader2);
        assertEquals(leader2, leader);

        cfg.restartExcept(leader2);
        String leader3 = cfg.checkOneLeader();
        assertNotNull(leader3);
        assertEquals(leader3, leader2);

        cfg.stop();
    }

    @Test
    public void testReElection3() throws InterruptedException {
        int host = 2;
        TestConfig cfg = TestConfig.make_config(host);
        String leader = cfg.checkOneLeader();
        assertNotNull(leader);

        cfg.disconnect(leader);
        String leader2 = cfg.checkOneLeader();
        assertNotNull(leader2);

        cfg.connect(leader);
        String leader3 = cfg.checkOneLeader();
        assertNotNull(leader3);
        assertEquals(leader3, leader2);

        cfg.stop();
    }

    @Test
    public void testLeaderCorrectness3() throws InterruptedException {
        int host = 3;
        TestConfig cfg = TestConfig.make_config(host);
        String leader = cfg.checkOneLeader();
        assertNotNull(leader);

        List<String> hosts = cfg.getAllConnectHost();
        hosts.remove(leader);
        String h1 = hosts.get(0);
        String h2 = hosts.get(1);


        cfg.disconnect(h2);
        Thread.sleep(1000);
        cfg.connect(h2);


        cfg.disconnect(leader);
        String newLeader = cfg.checkOneLeader();
        assertNotNull(newLeader);
        assertEquals(newLeader, h1);
        cfg.stop();
    }

    @Test
    public void testAllCrash3() throws InterruptedException {
        int host = 3;
        TestConfig cfg = TestConfig.make_config(host);
        String leader = cfg.checkOneLeader();
        assertNotNull(leader);

        cfg.disconnect(leader);
        String newLeader = cfg.checkOneLeader();
        assertNotNull(newLeader);

        cfg.disconnect(newLeader);
        newLeader = cfg.checkOneLeader();
        assertNotNull(newLeader);


        cfg.disconnect(newLeader);
        newLeader = cfg.checkOneLeader();
        assertNull(newLeader);

        cfg.stop();
    }

    @Test
    public void testReElection5() throws InterruptedException {
        int host = 5;

        TestConfig cfg = TestConfig.make_config(host);
        String leader = cfg.checkOneLeader();
        assertNotNull(leader);

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
        assertNotNull(newLeader);
        assertEquals(newLeader, hosts.get(3));

        cfg.disconnect(newLeader);
        cfg.disconnect(hosts.get(1));
        cfg.disconnect(hosts.get(2));
        Thread.sleep(1000);
        cfg.connect(hosts.get(1));
        cfg.connect(hosts.get(2));
        newLeader = cfg.checkOneLeader();
        assertNotNull(newLeader);
        assertEquals(newLeader, hosts.get(0));

        cfg.stop();
    }

}
