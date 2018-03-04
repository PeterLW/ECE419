package common.zookeeper;

import common.Metadata.Metadata;
import ecs.ServerNode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.concurrent.Semaphore;

/**
 IMPORTANT:
    Instructions:
        When KVServer Application first starts running, the 'main' thread starts up an instance of ZookeeperWatcher to
        connect to zookeeper
        It uses zookeeper watcher to get data from the znode with the same name as it
        (a KVServer must know it's name at start-up, perhaps passed in as a command line argument in the script?)

        After getting the data from the znode with the same name as it, the KVServer application has a ServerNode object.

        To get the data from the znode with the same name as it (the first time you do this from KVServer)
        use initServerNode().

        Then use setServerNode() to pass the same ServerNode object back into the Zookeeper Watcher object

        (If you use the KVServer  that I started to modify, look in the KVServer(...) constructor, much of this is already
        implemented (:
 */

public class ZookeeperWatcher extends ZookeeperManager implements Runnable {
    /*
        On each KVServer, this class is the zookeeper interface manager
        Instance one: helps to:
            get Data from Znode
            runs as a separate thread which will respond whenever data is changed by ECS
        Instance two (do not run this as another thread):
            get MetaData data
     */
    private static Logger LOGGER = Logger.getLogger(ZookeeperECSManager.class);
    private static String fullPath = null;
    private static ServerNode serverNode;
    /* ZookeeperWatcher needs to update serverNode whenever there's a new hash change, so must have link to it
        not actually 100% sure serverNode variable needed but we'll see...
     */

    private Semaphore semaphore = new Semaphore(1);

    public ZookeeperWatcher(String zookeeperHost, int sessionTimeout, String name) throws IOException, InterruptedException {
        super(zookeeperHost,sessionTimeout);
        fullPath = ZNODE_HEAD + "/" + name;
    }

    public ServerNode initServerNode() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(fullPath,false,null);
        String dataString = new String(data);
        ZNodeMessage newNode = gson.fromJson(dataString,ZNodeMessage.class);
        return newNode.serverNode;
    }

    public void setServerNode(ServerNode n){
        this.serverNode = n;
    }

    public Metadata getMetadata() throws KeeperException, InterruptedException {
        String metadataPath = ZNODE_HEAD + "/" + ZNODE_METADATA_NODE;
        zooKeeper.sync(metadataPath,null,null);
        byte[] data = zooKeeper.getData(metadataPath,false,null);
        String dataString = new String(data);
        Metadata n = gson.fromJson(dataString,Metadata.class);
        return n;
    }

    private void getNewData() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(fullPath, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    semaphore.release();
                }
            }
        }, null);

        ZNodeMessage temp = gson.fromJson(new String(data),ZNodeMessage.class);

        // in progress
        switch(temp.zNodeMessageStatus){
            case NEW_ZNODE:
                System.out.println("Data has changed");
                System.out.println(new String(data));
                break;
            case START_SERVER:
                System.out.println("Data has changed");
                System.out.println(new String(data));
                break;
            case STOP_SERVER:
                System.out.println("Data has changed");
                System.out.println(new String(data));
                break;
            case SHUTDOWN_SERVER:
                System.out.println("Data has changed");
                System.out.println(new String(data));
                break;
            case HASH_RANGE_UPDATE:
                System.out.println("Data has changed");
                System.out.println(new String(data));
                break;
        }

    }

    @Override
    public void run() {
        try {
            semaphore.acquire();
            while(true){
                try {
                    getNewData();
                    semaphore.acquire();
                } catch (KeeperException | InterruptedException e) {
                    LOGGER.error("Failed to get data from znode", e);
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire semaphore, ",e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperWatcher zookeeperWatcher = new ZookeeperWatcher("localhost:2181",10000,"TEST_SERVER_0 localhost");
        ServerNode n = zookeeperWatcher.initServerNode();
        zookeeperWatcher.setServerNode(n);
        zookeeperWatcher.run();
    }
}