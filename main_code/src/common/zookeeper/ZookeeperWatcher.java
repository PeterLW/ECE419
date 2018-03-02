package common.zookeeper;

import common.metadata.Metadata;
import ecs.ServerNode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.Semaphore;

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
    private static ServerNode serverNode; // ZookeeperWatcher needs to update serverNode whenever there's a new hash change, so must have link to it

    private Semaphore semaphore = new Semaphore(1);

    public ZookeeperWatcher(String zookeeperHost, int sessionTimeout, String name) throws IOException, InterruptedException {
        super(zookeeperHost,sessionTimeout);
        fullPath = ZNODE_HEAD + "/" + ZNODE_SERVER_PREFIX + name;
    }

    /**
     * call this first to get data (at KVserver starting to run)
     * It is not meant to set a watch
     */
    public ServerNode initServerNode() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(fullPath,false,null);
        String dataString = new String(data);
        ServerNode newNode = gson.fromJson(dataString,ServerNode.class);
        return newNode;
    }

    public void setServerNode(ServerNode n){
        this.serverNode = n;
    }

    public Metadata getMetadata() throws KeeperException, InterruptedException {
        String metadataPath = ZNODE_HEAD + "/" + ZNODE_CONFIG_NODE;
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

        ServerNode temp = gson.fromJson(new String(data),ServerNode.class);
        BigInteger newRange[] = temp.getRange();
        BigInteger oldRange[] = serverNode.getRange();

        if (newRange[0] != oldRange[0] || newRange[1] != oldRange[1]){
            // time to do updates!
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
}