package testing;

import common.zookeeper.HeartbeatTracker;
import common.zookeeper.ZookeeperECSManager;
import common.zookeeper.ZookeeperHeartbeat;
import common.zookeeper.ZookeeperHeartbeatWatcher;
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
            z.setHeartbeatTracker(ht);
            Thread c = new Thread(zm);
            c.start();

            ht.addServer("localhost:3000");
            ht.addServer("localhost:2000");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

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

}
