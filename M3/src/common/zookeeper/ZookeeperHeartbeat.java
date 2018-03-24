package common.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZookeeperHeartbeat extends ZookeeperManager implements Runnable {
    private String serverIpPort = null;

    public ZookeeperHeartbeat(String zookeeperHost, int sessionTimeout, String serverIpPort) throws IOException, InterruptedException, KeeperException {
        super(zookeeperHost, sessionTimeout); // put if statement - make function
        this.serverIpPort = serverIpPort;
        createQueueZnode();
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
        while (true) {
            addToQueue();
            try {
                Thread.sleep(60000); // 60s
            } catch (InterruptedException e) {
                System.out.println("sleep failed");
            }
        }
    }

    private void addToQueue(){
        try {
            zooKeeper.create(QUEUE_PATH + "/" + QUEUE_PREFIX, serverIpPort.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            System.out.println(serverIpPort + " added to Heartbeat Queue");
        } catch (KeeperException | InterruptedException e){
            System.out.println("Trying to add queue, failed. " + e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperHeartbeat zm = new ZookeeperHeartbeat("localhost:2191", 1000000,"localhost:2000"); // session timeout is in ms
        System.out.println(zm.isConnected());
        Thread a = new Thread(zm);
        a.start();
//        new ListGroupForever(zm.zooKeeper).listForever(ZNODE_HEAD); // debugging class
    }
}


