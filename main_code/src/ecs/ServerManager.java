package ecs;

import app_kvServer.ServerStatus;
import common.zookeeper.ZookeeperECSManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;

public class ServerManager {
//    // hash values (start) -> serverNode
//    private TreeMap<String,ServerNode> tree = new TreeMap<String,ServerNode>();

    // name: (serverName ip) -> serverNode
    private HashMap<String,ServerNode> hashMap = new HashMap<String,ServerNode>(); // stores the active running nodes

    private ZookeeperECSManager zookeeperManager;
    private static Logger LOGGER = Logger.getLogger(ServerManager.class);
    private int getNumOfServerConnected = 0;

    public ServerManager(){
        try {
            zookeeperManager = new ZookeeperECSManager("localhost:2181",10000); // session timeout ms
        } catch (Exception e) {
            LOGGER.error("Failed to connect to zookeeper. Check that zookeeper server has started and is running on localhost:2181");
            throw new RuntimeException("Failed to connect to zookeeper. Check that zookeeper server has started and is running on localhost:2181", e);
        }
    }

    public HashMap<String,ServerNode> getServerMap(){
        return hashMap;
    }

    public int getNumOfServerConnected(){

        return getNumOfServerConnected;
    }
    //this function will do the ssh thing.
    private void remoteLaunchServer(int portNum){

    }

    public boolean addNode(ServerNode n) throws KeeperException, InterruptedException {
        // have a default cache strategy & cache Size
        return addNode(n,"LRU",100);
    }


    public boolean addNode(ServerNode n, String cacheStrategy, int cacheSize) throws KeeperException, InterruptedException { // change to throw?
        String id = n.getNodeName();
        if (hashMap.containsKey(id)) {
            return false;
        }
        remoteLaunchServer(n.getNodePort());
        return true;
    }

    public boolean addKVServer(ServerNode n, String cacheStrategy, int cacheSize) throws KeeperException, InterruptedException { // change to throw?
        String id = n.getNodeName(); // ip:port
        if (hashMap.containsKey(id)) {
            return false;
        }
        zookeeperManager.addKVServer(n);
        hashMap.put(id,n);
        return true;
    }

    public boolean start(){
        return true;
    } // look into

    public boolean stop(){ // look into
        return true;
    } // do not delete nodes

    public boolean shutdown(){
        try {
            zookeeperManager.close();
            return true;
        } catch (InterruptedException e) {
            LOGGER.error("Zookeeper connection failed to close ",e);
            return false;
        }
    }

    public String getServerName(String Key){

        return null;
    }

    //ServerIndex: Ip addr + port number
    public void removeNode(String ServerIndex)throws KeeperException, InterruptedException{

        zookeeperManager.removeKVServer(ServerIndex);
        if (hashMap.containsKey(ServerIndex)){
            hashMap.remove(ServerIndex);
        } else {
            LOGGER.debug("Trying to remove server: " + ServerIndex + " but server not in hash map");
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ServerManager serverManager = new ServerManager();

        for (int i = 12; i<17; i++){
            String name = "SERVER_" + Integer.toString(i);
            int port = 1111+i;
            ServerNode n = new ServerNode(name,"localhost",port);
            boolean success = serverManager.addNode(n,"something", 100);
            System.out.println(success);
        }
        System.in.read();

//        for (int i = 0; i<5; i++){
//            String name = "TEST_SERVER_" + Integer.toString(i);
//            int port = 1111+i;
//            ServerNode n = new ServerNode(name,"localhost",port);
//            System.out.println("Servernode: " + n.getNodeName());
//            boolean success = serverManager.addKVServer(n,"something", 100);
//            System.out.println(success);
//        }
//        System.in.read();

        String name = "TEST_SERVER_" + Integer.toString(0);
        ServerNode n = new ServerNode(name,"localhost",1111+0);
        n.setRange(new BigInteger(String.valueOf(1111233)), new BigInteger(String.valueOf(11111111)));
        serverManager.zookeeperManager.addKVServer(n);

    }
}
