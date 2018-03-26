package testing;

import com.sun.security.ntlm.Server;
import common.cache.StorageManager;
import common.metadata.Metadata;
import common.zookeeper.HeartbeatTracker;
import common.zookeeper.ZookeeperECSManager;
import common.zookeeper.ZookeeperHeartbeat;
import common.zookeeper.ZookeeperHeartbeatWatcher;
import ecs.ConfigEntity;
import ecs.ServerManager;
import ecs.ServerNode;
import jdk.nashorn.internal.runtime.StoredScript;
import junit.framework.TestCase;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;

public class AdditionalTestsm3 extends TestCase {

    public void testZkClearNodes(){
        try {
            ZookeeperHeartbeatWatcher z = new ZookeeperHeartbeatWatcher("localhost:2191", 10000); // session timeout is in ms
            HeartbeatTracker ht = new HeartbeatTracker();
            z.setHeartbeatTracker(ht);
            assertTrue(z.isConnected());

            ZookeeperECSManager zk = new ZookeeperECSManager("localhost:2191", 10000);
            assertTrue(zk.isConnected());

            Metadata m = new Metadata();
            zk.updateMetadataZNode(m);

            assertTrue(z.isExistsQueueNode());
        } catch (Exception e){

        }
    }

    public void testZkHeartbeat(){
        try {
            ZookeeperHeartbeat zm = new ZookeeperHeartbeat("localhost:2191", 1000000, "localhost:2000"); // session timeout is in ms
            Thread a = new Thread(zm);
            a.start();

            ZookeeperHeartbeat zm2 = new ZookeeperHeartbeat("localhost:2191", 1000000, "localhost:3000"); // session timeout is in ms
            Thread b = new Thread(zm2);
            b.start();

            ZookeeperHeartbeatWatcher z = new ZookeeperHeartbeatWatcher("localhost:2191", 1000000); // session timeout is in ms
            HeartbeatTracker ht = new HeartbeatTracker();
            ht.addServer("localhost:3000");
            ht.addServer("localhost:2000");
            z.setHeartbeatTracker(ht);
            Thread c = new Thread(z);
            c.start();

            Thread.sleep(60000); // 1 min

            System.out.println(ht.getMin());
            assertTrue(ht.getMin().compareTo(new Integer(0)) != 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    // must start zookeeper first ============================
    public void testZkConnection(){
        try {
            ZookeeperHeartbeatWatcher z = new ZookeeperHeartbeatWatcher("localhost:2191", 1000000); // session timeout is in ms
            HeartbeatTracker ht = new HeartbeatTracker();
            z.setHeartbeatTracker(ht);

            assertTrue(z.isConnected());

            ZookeeperECSManager zk = new ZookeeperECSManager("localhost:2191", 1000000);
            assertTrue(zk.isConnected());
        } catch (Exception e){

        }

    }

    // must start zookeeper first =======================

    public void testZkAddServer() {
        try {
            ZookeeperHeartbeatWatcher z = new ZookeeperHeartbeatWatcher("localhost:2191", 1000000); // session timeout is in ms
            HeartbeatTracker ht = new HeartbeatTracker();
            z.setHeartbeatTracker(ht);

            assertTrue(z.isConnected());

            ZookeeperECSManager zk = new ZookeeperECSManager("localhost:2191", 1000000);
            assertTrue(zk.isConnected());

            ServerNode n = new ServerNode("localhost:5000", "localhost", 5000);
            zk.addKVServer(n);

            assertTrue(zk.shutdownKVServer(n));
        } catch (Exception e) {

        }
    }

    // must start zookeeper first ============================
    public void testZkHeartbeatDeadServer() {
        try {
            ZookeeperHeartbeat zm = new ZookeeperHeartbeat("localhost:2191", 1000000, "localhost:2000"); // session timeout is in ms
            Thread a = new Thread(zm);
            a.start();

            ZookeeperHeartbeat zm2 = new ZookeeperHeartbeat("localhost:2191", 1000000, "localhost:3000"); // session timeout is in ms
            Thread b = new Thread(zm2);
            b.start();

            ZookeeperHeartbeatWatcher z = new ZookeeperHeartbeatWatcher("localhost:2191", 1000000); // session timeout is in ms
            HeartbeatTracker ht = new HeartbeatTracker();
            ht.addServer("localhost:3000");
            ht.addServer("localhost:2000");
            ht.addServer("localhost:1000");
            z.setHeartbeatTracker(ht);
            Thread c = new Thread(z);
            c.start();

            Thread.sleep(240000); // 1 min
        } catch (Exception e) {
            assertEquals(e.getClass(), NullPointerException.class);
        }
    }

    public void testServerNode(){
        ConfigEntity ce = new ConfigEntity("server1","localhost",1000);
        assertEquals(0,ce.getHostName().compareTo("server1"));
        assertEquals(0,ce.getIpAddr().compareTo("localhost"));
        assertEquals(1000,ce.getPortNum());

        ServerNode sn = new ServerNode(ce,1000,"LRU");
        BigInteger[] bg = new BigInteger[2];
        bg[1] = new BigInteger("1");
        bg[0] = new BigInteger("11111");
        sn.setRange(bg);

        BigInteger[] bg2 = sn.getRange();

        assertEquals(0,bg[0].compareTo(bg2[0]));
        assertEquals(0,bg[1].compareTo(bg2[1]));
    }

    public void testServerNodeFromCE(){
        ConfigEntity ce = new ConfigEntity("server1","localhost",1000);
        assertEquals(0,ce.getHostName().compareTo("server1"));
        assertEquals(0,ce.getIpAddr().compareTo("localhost"));
        assertEquals(1000,ce.getPortNum());

        ServerNode sn = new ServerNode(ce,1000,"LRU");

        assertEquals(0,sn.getCacheStrategy().compareTo("LRU"));
        assertEquals(0,sn.getNodeHostPort().compareTo("localhost:1000"));
    }

    public void testConfigEntity(){
        ConfigEntity ce = new ConfigEntity("server1","localhost",1000);
        assertEquals(0,ce.getHostName().compareTo("server1"));
        assertEquals(0,ce.getIpAddr().compareTo("localhost"));
        assertEquals(1000,ce.getPortNum());
    }

    public void testRange(){
        StorageManager sm = new StorageManager(100,"FIFO","TEST_SERVER");
        for (int i = 0; i < 10; i++) {
            sm.putKV(Integer.toString(i), "b");
        }

        BigInteger[] range = new BigInteger[2];
        range[0] = new BigInteger("0");
        range[1] = new BigInteger("190188081314515644627836686569786975555");
        ArrayList<String> aList = sm.returnKeysInRange(range);

        assertTrue(aList.contains("6"));
        assertTrue(aList.contains("9"));
    }


}
