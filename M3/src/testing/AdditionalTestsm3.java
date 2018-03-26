package testing;

import common.zookeeper.HeartbeatTracker;
import common.zookeeper.ZookeeperECSManager;
import common.zookeeper.ZookeeperHeartbeat;
import common.zookeeper.ZookeeperHeartbeatWatcher;
import ecs.ServerNode;
import junit.framework.TestCase;

public class AdditionalTestsm3 extends TestCase {

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

    // must start zookeeper first
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

    // must start zookeeper first
    public void testZkAddServer(){
        try {
            ZookeeperHeartbeatWatcher z = new ZookeeperHeartbeatWatcher("localhost:2191", 1000000); // session timeout is in ms
            HeartbeatTracker ht = new HeartbeatTracker();
            z.setHeartbeatTracker(ht);

            assertTrue(z.isConnected());

            ZookeeperECSManager zk = new ZookeeperECSManager("localhost:2191", 1000000);
            assertTrue(zk.isConnected());

            ServerNode n = new ServerNode("localhost:5000","localhost", 5000);
            zk.addKVServer(n);

            assertTrue(zk.shutdownKVServer(n));
        } catch (Exception e){

        }

    }

}
