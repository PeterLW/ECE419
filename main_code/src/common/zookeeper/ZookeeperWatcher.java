package common.zookeeper;

import ecs.ServerNode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

public class ZookeeperWatcher extends ZookeeperManager implements Runnable {
    private static Logger LOGGER = Logger.getLogger(ZookeeperECSManager.class);
    private String fullPath = null;
    private Semaphore semaphore = new Semaphore(1);


    public ZookeeperWatcher(String zookeeperHost, int sessionTimeout, String name) throws IOException, InterruptedException {
        super(zookeeperHost,sessionTimeout);
        fullPath = ZNODE_HEAD + "/" + ZNODE_SERVER_PREFIX + name;
    }

    /**
     * call this first to get data (at KVserver starting to run)
     * It is not meant to set a watch
     * @return
     */
    public ServerNode getDataFromZnode() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(fullPath,false,null);
        String dataString = new String(data);
        ServerNode n = gson.fromJson(dataString,ServerNode.class);
        return n;
    }

    public void setWatch() throws KeeperException, InterruptedException {
        semaphore.acquire();
        byte[] data = zooKeeper.getData(fullPath, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    semaphore.release();
                }
            }
        }, null);
    }

    @Override
    public void run() {
        try {
            semaphore.acquire();
            while(true){
                
                semaphore.acquire();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire semaphore, ",e);
        }
    }
}