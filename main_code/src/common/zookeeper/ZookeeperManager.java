package common.zookeeper;

import ecs.ServerNode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperManager {
    private static Logger LOGGER = Logger.getLogger(ZookeeperManager.class);

    private static final String HEAD_NAME = "ZNODES_HEAD";
    private static final String ZNODE_HEAD = "/"+HEAD_NAME;
    private static final String ZNODE_CONFIG_NODE = "CONFIG_DATA";
    private static final String ZNODE_SERVER_PREFIX = "KVSERVER_";

    private ZooKeeper zooKeeper = null;

    /**
     * Connects to zookeeper server
     * @param zookeeperHosts ip:port of zookeeper server
     * @param sessionTimeout
     * @throws IOException
     * @throws InterruptedException
     */
    public ZookeeperManager(String zookeeperHosts, int sessionTimeout)
            throws IOException, InterruptedException, KeeperException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(zookeeperHosts,sessionTimeout,
                new Watcher(){
                    @Override
                    public void process(WatchedEvent event){
                        if(event.getState() == Event.KeeperState.SyncConnected){
                            connectedSignal.countDown();
                        }
                    }
                });
        connectedSignal.await();
        System.out.println("connected");
        clearZNodes(); // in case crashed before shutting down last time
        createHead();
        addZNode(ZNODE_HEAD, ZNODE_CONFIG_NODE,null);
        test();
    }

    private void createHead() throws KeeperException, InterruptedException {
        zooKeeper.create(ZNODE_HEAD,null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public boolean isConnected(){
        if (zooKeeper != null){
            return true;
        }
        return false;
    }

    public boolean addKVServer(ServerNode n){


        this.addZNode(ZNODE_HEAD,n.getNodeName(),);
        return true;
    }

    private void addZNode(String path, String memberName, byte[] data) throws KeeperException, InterruptedException {
        String fullPath = path + "/" + memberName;
        String createdPath = zooKeeper.create(fullPath,data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public void close(){
        clearZNodes();
    }


    private void test(){
    }


    private boolean deleteZNode(String path, String groupName){
        String fullPath = path + "/" + groupName;
        try {
            Stat stat = zooKeeper.exists(fullPath, false);
            if (stat == null)
                return true;
            List<String> children = zooKeeper.getChildren(fullPath, false);
            for (String child : children) {
                zooKeeper.delete(fullPath + "/" + child, -1);
            }
            zooKeeper.delete(fullPath, -1);
            return true;
        } catch (KeeperException.NoNodeException e) {
            System.out.printf("Group %s does not exist\n", fullPath);
            return false;
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void clearZNodes(){ // zookeeper only has one layer, no need recursion
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

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperManager zm = new ZookeeperManager("localhost:2181", 10000); // session timeout is in ms
        System.out.println(zm.isConnected());
        zm.addZNode(ZNODE_HEAD,"wee",null);
        new ListGroupForever(zm.zooKeeper).listForever(HEAD_NAME); // debugging class
    }

}




