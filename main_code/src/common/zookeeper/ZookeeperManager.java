package common.zookeeper;

import com.google.gson.Gson;
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
    private static final Gson gson = new Gson();


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

    public void addKVServer(ServerNode n) throws KeeperException, InterruptedException {
        String jsonServerData = gson.toJson(n);
        System.out.println(jsonServerData);
        this.addZNode(ZNODE_HEAD,n.getNodeName(),toByteArray(jsonServerData));
    }

    public void removeKVServer(ServerNode n) {
        this.deleteZNode(ZNODE_HEAD,n.getNodeName());
    }

    public void getServerDataAndAddWatch(String serverName) throws KeeperException, InterruptedException {
        String fullPath = ZNODE_HEAD + "/" + serverName;
        Stat stat = zooKeeper.exists(fullPath,false);
        if (stat == null){
            LOGGER.debug("Attempting to access data for server: " + serverName + " but no znode created of that name");
            return;
        }

//        byte[] data = zooKeeper.getData(fullPath,
//                new Watcher() {
//            @Override
//            public void process(WatchedEvent watchedEvent) {
//                if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
//                    System.out.println("temp");
//
//                    }
//                }
//            });
    }


    private void addZNode(String path, String memberName, byte[] data) throws KeeperException, InterruptedException { // KeeperException can be thrown if data to large
        String fullPath = path + "/" + memberName;
        if (zooKeeper.exists(fullPath,false) == null) {
            zooKeeper.create(fullPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            LOGGER.debug("Successfully created znode: " + fullPath);
        } else {
            LOGGER.debug("Trying to add znode: " + fullPath + " ,but already exists, updating data instead");
            zooKeeper.setData(fullPath,data,-1);
        }
    }

    public void close(){
        clearZNodes();
        // how to close connection with zookeeper?
    }

    private void deleteZNode(String path, String groupName){
        String fullPath = path + "/" + groupName;
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
        } catch (InterruptedException | KeeperException e) {
            LOGGER.error(e);
            throw new RuntimeException("Error trying to delete: " + fullPath + " ,",e);
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

    private byte[] toByteArray(String s) {
        byte[] bytes = s.getBytes();
        byte[] tmp = new byte[bytes.length];
        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        return tmp;
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperManager zm = new ZookeeperManager("localhost:2181", 10000); // session timeout is in ms
        System.out.println(zm.isConnected());
        zm.addZNode(ZNODE_HEAD,"wee",null);
        new ListGroupForever(zm.zooKeeper).listForever(HEAD_NAME); // debugging class
    }

}




