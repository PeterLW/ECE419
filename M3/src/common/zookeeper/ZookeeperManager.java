package common.zookeeper;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperManager {
    private static Logger LOGGER = Logger.getLogger(ZookeeperECSManager.class);
    protected static final String HEAD_NAME = "ZNODES_HEAD";
    protected static final String ZNODE_HEAD = "/"+HEAD_NAME;
    protected static final String ZNODE_METADATA_NODE = "METADATA_NODE";
    protected static final String SERVER_STATUS_NODE = "SERVER_STATUS_NODE";
    protected static final Gson gson = new Gson();

    protected static final String QUEUE_NAME = "QUEUE_ZNODE";
    protected static final String QUEUE_PATH = ZNODE_HEAD + "/" + QUEUE_NAME;
    protected static final String QUEUE_PREFIX = "QUEUE";

    protected ZooKeeper zooKeeper = null;

    public ZookeeperManager(String zookeeperHost, int sessionTimeout) throws IOException, InterruptedException {
        if (zooKeeper == null) {
            connect(zookeeperHost, sessionTimeout);
            System.out.println("Connected to zookeeper.");
        } else {
            System.out.println("Already connected to zookeeper.");
        }
//        zooKeeper.sync(ZNODE_HEAD,null,null);
    }

    private void connect(String zookeeperHost, int sessionTimeout) throws IOException, InterruptedException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(zookeeperHost,sessionTimeout,
                new Watcher(){
                    @Override
                    public void process(WatchedEvent event){
                        if(event.getState() == Event.KeeperState.SyncConnected){
                            connectedSignal.countDown();
                        }
                    }
                });
        connectedSignal.await();
    }

    public boolean isConnected(){
        if (zooKeeper != null){
            return true;
        }
        return false;
    }


    protected void clearZNodes(){ // zookeeper only has one layer, no need recursion
        try {
            Stat stat = zooKeeper.exists(ZNODE_HEAD, false);
            if (stat == null)
                return;
            List<String> children = zooKeeper.getChildren(ZNODE_HEAD, false);
            for (String child : children) {
                zooKeeper.delete(ZNODE_HEAD + "/" + child, -1);
            }
            zooKeeper.delete(ZNODE_HEAD, -1);
        } catch (Exception e) {
            LOGGER.error("Error deleting all nodes from zookeeper");
            throw new RuntimeException("Error deleting all nodes from zookeeper",e);
        }
    }



    protected void createHead() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(ZNODE_HEAD,false);
        if (stat == null) {
            zooKeeper.create(ZNODE_HEAD, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.debug("Successfully created Head node");
            System.out.println("Successfully created Head node");
        } else {
            LOGGER.debug("Head already exists, not creating");
            System.out.println("Head already exists, not creating");
        }
    }

    protected void addZNode(String path, String memberName, byte[] data) throws KeeperException, InterruptedException { // KeeperException can be thrown if data to large
        String fullPath = path + "/" + memberName;
        zooKeeper.sync(fullPath,null,null);
        Stat stat = zooKeeper.exists(fullPath,false);
        if (stat == null) {
            zooKeeper.create(fullPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //zooKeeper.setData(fullPath,data,-1);
            LOGGER.debug("Successfully created znode: " + fullPath);
            System.out.println("Successfully created znode: " + fullPath);
        } else {
            LOGGER.debug("Trying to add znode: " + fullPath + " ,but already exists, updating data instead");
            System.out.println("Trying to add znode: " + fullPath + " ,but already exists, updating data instead");
            System.out.println("stat: " + stat.toString());
            zooKeeper.setData(fullPath,data,-1);
        }
    }


    protected void deleteZNode(String path, String groupName) throws KeeperException, InterruptedException {
        String fullPath = path + "/" + groupName;
        deleteZNode(fullPath);
    }

    protected void deleteZNode (String fullPath) throws KeeperException, InterruptedException {
        try {
            Stat stat = zooKeeper.exists(fullPath, false);
            if (stat == null) {
                LOGGER.debug("Attempting to delete znode: " + fullPath + " but znode does not exist");
                return;
            }
            List<String> children = zooKeeper.getChildren(fullPath, false);
            for (String child : children) {
                zooKeeper.delete(fullPath + "/" + child, -1);
            }
            zooKeeper.delete(fullPath, -1);
        } catch (KeeperException.NoNodeException e) {
            LOGGER.error("Trying to delete: " + fullPath + " but Znode does not exist\n", e);
        }
    }

    protected void close() throws InterruptedException {
        zooKeeper.close();
    }

    protected void shutdown() throws InterruptedException {
        this.close();
        System.exit(0);
    }
}
