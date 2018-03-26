package common.zookeeper;

import ecs.ServerManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * To use, must add server to heartbeat tracker from ECSClient before heartbeat tracker will record
 * heartbeats from server
 */
public class ZookeeperHeartbeatWatcher extends ZookeeperManager implements Runnable {
    private Semaphore semaphore = new Semaphore(1);
    private ServerManager serverManager = null; // reference
    private HeartbeatTracker heartbeatTracker = null; // reference

    public ZookeeperHeartbeatWatcher(String zookeeperHost, int sessionTimeout) throws IOException, InterruptedException, KeeperException {
        super(zookeeperHost, sessionTimeout); // put if statement - make function
        clearZNodes(); // in case crashed before shutting down last time
        createQueueZnode();
    }

    public void setServerManager(ServerManager sm){
        serverManager = sm;
    }

    public void setHeartbeatTracker(HeartbeatTracker ht){
        heartbeatTracker = ht;
    }

    public boolean isExistsQueueNode() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(ZNODE_HEAD+"/"+QUEUE_NAME,false);
        return (stat != null);
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
            System.out.println("Running");
            while (true) {
                checkHeartbeats();
                semaphore.acquire();
            }
        } catch (KeeperException | InterruptedException e) {
            System.out.println("Check heartbeats failed ");
            e.printStackTrace();
        }
    }

    private void checkHeartbeats() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(QUEUE_PATH,  new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    semaphore.release();
                }
            }
        });

        if (children.isEmpty()) {
            System.out.printf("~No members in group %s\n", QUEUE_PATH);
            return;
        }

        for (String zNodeName : children){
            byte[] data = zooKeeper.getData(QUEUE_PATH+"/"+zNodeName,null,null);
            String serverIpPort = new String(data);
//            System.out.print(" " + serverIpPort + " | ");
            if (!heartbeatTracker.appendCount(serverIpPort)) {
                System.out.print(serverIpPort + " appendCount failed - check if added to heartbeatTracker |");
            }
            zooKeeper.delete(QUEUE_PATH+"/"+zNodeName,-1);

        }
        System.out.println(" \n" + children);

        String value = heartbeatTracker.getDead();
        if (value == null){ // no dead
            return;
        }

        System.out.println("Found a dead node: " + value + " starting removeNode procedure for a already dead node.");
        // call servermanager.removeNode() -
        serverManager.removeNode(value, true); // should be serverIpPort

        // clean-up
        heartbeatTracker.removeServer(value);
        Stat stat = zooKeeper.exists(ZNODE_HEAD+"/" + value,null);
        if (stat != null){
            zooKeeper.delete(ZNODE_HEAD+"/"+value,-1);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperHeartbeatWatcher zm = new ZookeeperHeartbeatWatcher("localhost:2191", 1000000); // session timeout is in ms
        HeartbeatTracker ht = new HeartbeatTracker();
        ht.addServer("localhost:4000");
        zm.setHeartbeatTracker(ht);
        System.out.println(zm.isConnected());
        Thread a = new Thread(zm);
        a.start();

        System.in.read();
        ht.addServer("localhost:3000");
        ht.addServer("localhost:2000");
//        new ListGroupForever(zm.zooKeeper).listForever(ZookeeperManager.QUEUE_PATH); // debugging class
    }
}
