package common.zookeeper;

import ecs.ServerManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

public class ZookeeperHeartbeatWatcher extends ZookeeperManager implements Runnable {
    private Semaphore semaphore = new Semaphore(1);
    private ServerManager serverManager = null; // reference
    private HeartbeatTracker heartbeatTracker = null; // reference

    public ZookeeperHeartbeatWatcher(String zookeeperHost, int sessionTimeout) throws IOException, InterruptedException, KeeperException {
        super(zookeeperHost, sessionTimeout); // put if statement - make function
        createQueueZnode();
    }

    public void setServerManager(ServerManager sm){
        serverManager = sm;
    }

    public void setHeartbeatTracker(HeartbeatTracker ht){
        heartbeatTracker = ht;
    }

    private void createQueueZnode() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(ZNODE_HEAD,false);
        if (stat == null){
            createHead();
        }

        addZNode(ZNODE_HEAD,QUEUE_NAME,null); // create queue node
    }


    @Override
    public void run() {
        try {
            semaphore.acquire();
            while (true) {
                checkHeartbeats();
                semaphore.acquire();
            }
        } catch (KeeperException | InterruptedException e) {
            System.out.println("Check heartbeats failed");
        }
    }

    private void checkHeartbeats() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(QUEUE_PATH, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    semaphore.release();
                }
            }
        });

        if (children.isEmpty()) {
            System.out.printf("No members in group %s\n", QUEUE_PATH);
            return;
        }

        for (String zNodeName : children){
            byte[] data = zooKeeper.getData(QUEUE_PATH+"/"+zNodeName,null,null);
            String serverIpPort = data.toString();
            heartbeatTracker.appendCount(serverIpPort);
            zooKeeper.delete(QUEUE_PATH+"/"+zNodeName,-1);
        }

        String value = heartbeatTracker.getDead();
        if (value == null){ // no dead
            return;
        }

        System.out.println("Found a dead node: " + value + " starting removeNode procedure for a already dead node.");
        // call servermanager.removeNode() -
        serverManager.removeNode(value); // should be serverIpPort
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperHeartbeatWatcher zm = new ZookeeperHeartbeatWatcher("localhost:2191", 1000000); // session timeout is in ms
        System.out.println(zm.isConnected());
        new ListGroupForever(zm.zooKeeper).listForever(ZookeeperManager.QUEUE_PATH); // debugging class
    }
}
