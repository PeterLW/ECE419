package common.zookeeper;

import ecs.ServerNode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class ZookeeperECSManager extends ZookeeperManager{
    /*
        class for managing zookeeper for the ECSClient
     */
    private static Logger LOGGER = Logger.getLogger(ZookeeperECSManager.class);

    /**
     * Connects to zookeeper server
     * @param zookeeperHosts ip:port of zookeeper server
     * @param sessionTimeout
     * @throws IOException
     * @throws InterruptedException
     */
    public ZookeeperECSManager(String zookeeperHosts, int sessionTimeout)
            throws IOException, InterruptedException, KeeperException {
        super(zookeeperHosts,sessionTimeout);
        System.out.println("Connected to Zookeeper client " + zookeeperHosts);
        LOGGER.info("Connected to Zookeeper client " + zookeeperHosts);
//        clearZNodes(); // in case crashed before shutting down last time
        createHead();
        // the config node should store the metadata class
        addZNode(ZNODE_HEAD, ZNODE_CONFIG_NODE,null);
    }

    private void createHead() throws KeeperException, InterruptedException {
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

    public void addKVServer(ServerNode n) throws KeeperException, InterruptedException {
        ZNodeMessage message = new ZNodeMessage(n,UpdateType.NEW_ZNODE);
        String jsonServerData = gson.toJson(message);
        System.out.println(jsonServerData); // debug
        this.addZNode(ZNODE_HEAD,n.getNodeName(),toByteArray(jsonServerData));
    }

    public void updateRange(ServerNode n) throws KeeperException, InterruptedException {
        ZNodeMessage message = new ZNodeMessage(n,UpdateType.HASH_RANGE_UPDATE);
        String jsonServerData = gson.toJson(message);
        System.out.println(jsonServerData); // debug
        this.updateZNode(ZNODE_HEAD,n.getNodeName(),toByteArray(jsonServerData));
    }

    public void startKVServer(ServerNode n) throws KeeperException, InterruptedException {
        ZNodeMessage message = new ZNodeMessage(n,UpdateType.START_SERVER);
        String jsonServerData = gson.toJson(message);
        System.out.println(jsonServerData); // debug
        this.updateZNode(ZNODE_HEAD,n.getNodeName(),toByteArray(jsonServerData));
    }

    public void stopKVServer(ServerNode n) throws KeeperException, InterruptedException {
        ZNodeMessage message = new ZNodeMessage(n,UpdateType.STOP_SERVER);
        String jsonServerData = gson.toJson(message);
        System.out.println(jsonServerData); // debug
        this.updateZNode(ZNODE_HEAD,n.getNodeName(),toByteArray(jsonServerData));
    }

    public void shutdownKVServer(ServerNode n) throws KeeperException, InterruptedException {
        ZNodeMessage message = new ZNodeMessage(n,UpdateType.SHUTDOWN_SERVER);
        String jsonServerData = gson.toJson(message);
        System.out.println(jsonServerData); // debug
        this.updateZNode(ZNODE_HEAD,n.getNodeName(),toByteArray(jsonServerData));
    }

    public void removeKVServer(String name) throws KeeperException, InterruptedException {
        this.deleteZNode(ZNODE_HEAD,name);
    }

    public void close() throws InterruptedException {
        clearZNodes();
        super.close();
    }

    private boolean updateZNode(String path, String memberName, byte[] data) throws KeeperException, InterruptedException {
        String fullPath = path + "/" + memberName;
        Stat stat = zooKeeper.exists(fullPath, false);
        if (stat != null){
            zooKeeper.setData(fullPath,data,-1);
            return true;
        } else {
            LOGGER.error("Attempting to update znode: " + fullPath + " , however it does not exist");
            return false;
        }
    }

    private void addZNode(String path, String memberName, byte[] data) throws KeeperException, InterruptedException { // KeeperException can be thrown if data to large
        String fullPath = path + "/" + memberName;
        zooKeeper.sync(fullPath,null,null);
        Stat stat =zooKeeper.exists(fullPath,false);
        if (stat == null) {
            zooKeeper.create(fullPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.debug("Successfully created znode: " + fullPath);
            System.out.println("Successfully created znode: " + fullPath);
        } else {
            LOGGER.debug("Trying to add znode: " + fullPath + " ,but already exists, updating data instead");
            System.out.println("Trying to add znode: " + fullPath + " ,but already exists, updating data instead");
            zooKeeper.setData(fullPath,data,-1);
        }
    }

    private void deleteZNode(String path, String groupName) throws KeeperException, InterruptedException {
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
        ZookeeperECSManager zm = new ZookeeperECSManager("localhost:2181", 1000000); // session timeout is in ms
        System.out.println(zm.isConnected());
        new ListGroupForever(zm.zooKeeper).listForever(ZNODE_HEAD); // debugging class
    }

}



