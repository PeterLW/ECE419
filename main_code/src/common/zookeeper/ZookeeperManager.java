package common.zookeeper;

import com.google.gson.Gson;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZookeeperManager {
    protected static final String HEAD_NAME = "ZNODES_HEAD";
    protected static final String ZNODE_HEAD = "/"+HEAD_NAME;
    protected static final String ZNODE_METADATA_NODE = "METADATA_NODE";
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

    protected void close() throws InterruptedException {
        zooKeeper.close();
    }

    protected void shutdown() throws InterruptedException {
        this.close();
        System.exit(0);
    }
}
