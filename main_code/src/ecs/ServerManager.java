package ecs;

import common.zookeeper.ZookeeperManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashMap;

public class ServerManager {
//    // hash values (start) -> serverNode
//    private TreeMap<String,ServerNode> tree = new TreeMap<String,ServerNode>();
    // ip:port -> serverNode
    private HashMap<String,ServerNode> hashMap = new HashMap<String,ServerNode>();

    private ZookeeperManager zookeeperManager;
    private static Logger LOGGER = Logger.getLogger(ServerManager.class);

    public ServerManager(){
        try {
            zookeeperManager = new ZookeeperManager("localhost:2181",10000); // session timeout ms
        } catch (Exception e) {
            LOGGER.error("Failed to connect to zookeeper. Check that zookeeper server has started and is running on localhost:2181");
            throw new RuntimeException("Failed to connect to zookeeper. Check that zookeeper server has started and is running on localhost:2181", e);
        }
    }

    public HashMap<String,ServerNode> getServerMap(){
        return hashMap;
    }

    public boolean addKVServer(ServerNode n) throws KeeperException, InterruptedException {
        // have a default cache strategy & cache Size
        return addKVServer(n,"LRU",100);
    }

    public boolean addKVServer(ServerNode n, String cacheStrategy, int cacheSize) throws KeeperException, InterruptedException { // change to throw?
        String id = n.getNodeId(); // ip:port
        if (hashMap.containsKey(id)) {
            return false;
        }

        zookeeperManager.addKVServer(n);
        hashMap.put(id,n);
        return true;
    }

    public boolean shutdown(){
        return true;
    }

    public boolean start(){
        return true;
    }

    public boolean stop(){
        return true;
    }

    public void removeKVServer(ServerNode n) throws KeeperException, InterruptedException {

        String id = n.getNodeId();
        zookeeperManager.removeKVServer(n);
        if (hashMap.containsKey(id)){
            hashMap.remove(id);
        } else {
            LOGGER.debug("Trying to remove server: " + n.getServerName() + " id: " + n.getNodeId() + " but server not in hash map");
        }
    }

    public void removeNode(String NodeName)throws KeeperException, InterruptedException{

    }
    public ServerNode getNodeByKey(String Key) {
        return null;
    }

    public void close(){
        zookeeperManager.close();
    }


//    public boolean removeNode(String id) {
//        ServerNode n = hashMap.get(id);
//        if (n == null){
//            return false;
//        }
//        String hash = n.getNodeHashRange()[0];
//
//        if(!tree.containsKey(hash)){
//            hashMap.remove(id);
//            throw new RuntimeException(id + " was found in hashMap but not in tree");
//        }
//
//        hashMap.remove(id);
//        tree.remove(hash);
//        return true;
//    }
//
//    /**
//     * @return number of nodes successfully removed
//     */
//    public int removeNodes(List<String> ids){
//        return 0;
//    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ServerManager serverManager = new ServerManager();
        for (int i = 12; i<17; i++){
            String name = "SERVER_" + Integer.toString(i);
            int port = 1111+i;
            ServerNode n = new ServerNode(name,"localhost",port);
            boolean success = serverManager.addKVServer(n,"something", 100);
            System.out.println(success);
        }
        System.in.read();
    }
}
