package common.zookeeper;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
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

    protected ZooKeeper zooKeeper = null;

    public ZookeeperManager(String zookeeperHost, int sessionTimeout) throws IOException, InterruptedException {
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
        zooKeeper.sync(ZNODE_HEAD,null,null);
    }

    public boolean isConnected(){
        if (zooKeeper != null){
            return true;
        }
        return false;
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
