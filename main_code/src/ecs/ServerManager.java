package ecs;

import common.metadata.Metadata;
import common.zookeeper.ZookeeperECSManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.io.*;

public class ServerManager {
    private static final Logger LOGGER = Logger.getLogger(ServerManager.class);

    private static final String RELATIVE_CONFIG_FILE_PATH = "/src/app_kvECS/ecs.config"; // SHOULD BE an argument passed in at start-up
    
    private static Metadata metadata = new Metadata();

    /* It was stated we can assume zookeeper running on same machine, default port*/
    private static String ZOOKEEPER_HOST_NAME = "localhost";
    private static String ZOOKEEPER_PORT = "2191";
    private static String ZOOKEEPER_HOST_PORT = ZOOKEEPER_HOST_NAME + ":" + ZOOKEEPER_PORT;
   
    private LinkedHashMap<String,IECSNode> hashMap = new LinkedHashMap<String,IECSNode>(); // stores the active running nodes
    private LinkedList<ConfigEntity> entityList = new LinkedList<ConfigEntity>(); // stores provided nodes from Config file
    private ZookeeperECSManager zookeeperECSManager;
    private boolean is_init;

    public ServerManager(){
        try {
            zookeeperECSManager = new ZookeeperECSManager(ZOOKEEPER_HOST_PORT,10000); // session timeout ms
            System.out.println("Working Directory = " + System.getProperty("user.dir"));
            // start parseConfigFile with path to file
            String filePath =new File(".").getCanonicalPath()+RELATIVE_CONFIG_FILE_PATH;
            parseConfigFile(filePath);
        } catch (Exception e) {
            LOGGER.error("Failed to connect to zookeeper. Check that zookeeper server has started and is running on " + ZOOKEEPER_PORT );
            throw new RuntimeException("Failed to connect to zookeeper. Check that zookeeper server has started and is running on localhost:2181", e);
        }
    }

    public void setZkParams(String host, String port){ // for autotester
        ZOOKEEPER_PORT = port;
        ZOOKEEPER_HOST_NAME = host;
    }

    public HashMap<String,IECSNode> getServerMap(){
        return hashMap;
    }

    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize){
        LinkedList<IECSNode>list = new LinkedList<IECSNode>();
        try {
            if(hashMap.isEmpty()) { // init stage
                this.is_init = true;
                for (int i = 0; i < count; ++i) { // step 1: completely initialize metadata
                    ServerNode node = new ServerNode(entityList.removeFirst(), cacheSize, cacheStrategy);
                    list.add(node);
                    metadata.addServer(node.getNodeHostPort());
                }
                zookeeperECSManager.updateMetadataZNode(metadata); // step 2
               
                for(int i = 0; i < count; ++i){ // step 3: launch
                    ServerNode node = (ServerNode) list.get(i);

                    // now when I add zNode they have their range given a -full- hash ring.
                    this.addServer(node, cacheStrategy, cacheSize);
                    System.out.println("node.serverName = "+node.getNodeName() + ", node.ipport = " +node.getNodeHostPort());

                    SSH ssh_conn = new SSH(node.getNodeHostPort(), ZOOKEEPER_HOST_NAME,ZOOKEEPER_PORT);
                    Thread sshConnThread = new Thread(ssh_conn);
                    sshConnThread.start();
                }
            }
            else{
                this.is_init = false;
                for(int i = 0; i < count; ++i){
                    ServerNode node = new ServerNode(entityList.removeFirst(), cacheSize, cacheStrategy);
                    metadata.addServer(node.getNodeHostPort()); // add and set
                    node.setRange(metadata.findHashRange(node.getNodeHostPort()));
                    list.add(node);

                    zookeeperECSManager.updateMetadataZNode(metadata); // update metadata node
                    this.addServer(node, cacheStrategy, cacheSize);
                    System.out.println("node.serverName = "+node.getNodeName() + ", node.ipport = " +node.getNodeHostPort());
                
                    SSH ssh_conn = new SSH(node.getNodeHostPort(), ZOOKEEPER_HOST_NAME,ZOOKEEPER_PORT);
                    Thread sshConnThread = new Thread(ssh_conn);
                    sshConnThread.start();
                }
            }
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error("Failed to update metadata");
            e.printStackTrace();
            return null;
        }
        return list;

    }


    //update the hashmap in ServerManager
    private ServerNode updateSuccessor(ServerNode node){
        BigInteger[] range = metadata.findHashRange(node.getNodeHostPort());
        if(range == null) {
            return null;
        }

        String successorID = metadata.getSuccessor(node.getNodeHostPort());
        if (successorID == null){
            return null;
        }

        BigInteger[] successorRange = metadata.findHashRange(successorID);
        if (successorRange == null){
            return null;
        }

        ServerNode updatedNode = updateSuccessorNode(successorID, successorRange);
        return updatedNode;

    }

    private ServerNode updateSuccessorNode(String id, BigInteger[] newRange){ // renamed fct didn't change code
        ServerNode node = null;
        for (Map.Entry<String, IECSNode> entry : hashMap.entrySet()) {
            node = (ServerNode) entry.getValue();
            if(node.getNodeHostPort().equals(id)){
                node.setRange(newRange[0],newRange[1]); // this line change the node instance in hash map,
                // don't need to put it back
                // hashMap.put(id, node);
                break;
            }
        }
        return node;
    }

    /**
     * add server into correct data structures
     */

    private boolean addServer(ServerNode n, String cacheStrategy, int cacheSize) throws KeeperException, InterruptedException { // change to throw?

        String id = n.getNodeHostPort();
        if (hashMap.containsKey(id)) {
            return false;
        }

        BigInteger[] newRange = metadata.findHashRange(id);
        n.setRange(newRange);
        hashMap.put(id,n);

        try {
            if(!is_init) {
                ServerNode successor = updateSuccessor(n);
                if(successor != null) {
                    zookeeperECSManager.addAndMoveDataKVServer(n, newRange, successor.getNodeHostPort());
                    Thread.sleep(1);
                    zookeeperECSManager.moveDataSenderKVServer(successor, newRange, n.getNodeHostPort());
                }
                else{
                    LOGGER.error("Failed to find successor!");
                }
            }
            else{
                zookeeperECSManager.addKVServer(n); // add new Znode
            }
        }catch (KeeperException | InterruptedException e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public IECSNode getServerName(String Key){
        String[] temp = metadata.findServer(Key).split(":");
        Iterator i = hashMap.entrySet().iterator();

        while(i.hasNext()){
            Map.Entry pair = (Map.Entry)i.next();
            IECSNode n = (IECSNode)pair.getValue();
            if(temp[0].equals(n.getNodeHost()) && temp[1].equals(n.getNodePort())){
                return n;
            }
        }
        return null;
    }

    public boolean start(){
        for (Map.Entry<String, IECSNode> entry : hashMap.entrySet()) {
            ServerNode node = (ServerNode) entry.getValue();
            try {
                zookeeperECSManager.startKVServer(node);
            }catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public boolean stop(){
        for (Map.Entry<String, IECSNode> entry : hashMap.entrySet()) {
            ServerNode node = (ServerNode) entry.getValue();
            try {
                zookeeperECSManager.stopKVServer(node);
            }catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public boolean shutdown(){
        for (Map.Entry<String, IECSNode> entry : hashMap.entrySet()) {
            ServerNode node = (ServerNode) entry.getValue();
            try {
                zookeeperECSManager.shutdownKVServer(node);
            }catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public void close(){
        this.shutdown();
        try {
            zookeeperECSManager.close();
        } catch (InterruptedException e) {
            LOGGER.error("Zookeeper connection shutdown failed");
        }
    }

    //ServerIndex: ip:port
    public boolean removeNode(String ServerIndex)throws KeeperException, InterruptedException{
        if (hashMap.containsKey(ServerIndex)){
            ServerNode node = (ServerNode) hashMap.get(ServerIndex);
            //if remove node, add the node back to entity list for next launch
            ConfigEntity entity = new ConfigEntity(node.getNodeHost(), node.getNodeHost(), node.getNodePort());
            entityList.add(entity);

            BigInteger[] range = metadata.findHashRange(ServerIndex);
            ServerNode successor = updateSuccessor(node);

            metadata.removeServer((node).getNodeHostPort());
            hashMap.remove(ServerIndex);

            try {
                if (!hashMap.isEmpty()) {
                    zookeeperECSManager.moveDataReceiverKVServer(successor, range, node.getNodeHostPort());
                    Thread.sleep(1);
                    zookeeperECSManager.removeAndMoveDataKVServer(node, range, successor.getNodeHostPort());
                } else {
                    zookeeperECSManager.shutdownKVServer(node);
                }
            }catch (KeeperException | InterruptedException e){
                e.printStackTrace();
                return false;
            }
            return true;
        }
        else {
            LOGGER.error("Cannot remove non-existing node!");
            return false;
        }
    }


    /**
     * parse the ecs.config file to get a list of IPs
     * @return a string array containing info regarding one machine
     */
    private void parseConfigFile(String filePath){
        try {
            File file = new File(filePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            StringBuffer stringBuffer = new StringBuffer();

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuffer.append(line);
                stringBuffer.append(",");
            }
            fileReader.close();
            String machine_list = stringBuffer.toString();
            String[] splitArray = machine_list.split(",");

            int length = splitArray.length;
            for(int i = 0; i < length; i++){
                String[] entry = splitArray[i].split("\\s+");
                ConfigEntity node = new ConfigEntity(entry[0],entry[1],Integer.parseInt(entry[2]));
                entityList.add(node);
            }
            bufferedReader.close();
        } catch (IOException e) {
            LOGGER.error("Error parsing config file", e);
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
//        ServerManager serverManager = new ServerManager();
//
//        for (int i = 12; i<17; i++){
//            String name = "SERVER_" + Integer.toString(i);
//            int port = 1111+i;
//            ServerNode n = new ServerNode(name,"localhost",port);
//           // boolean success = serverManager.addNode(n,"something", 100);
//           // System.out.println(success);
//        }
//        System.in.read();
//
////        for (int i = 0; i<5; i++){
////            String name = "TEST_SERVER_" + Integer.toString(i);
////            int port = 1111+i;
////            ServerNode n = new ServerNode(name,"localhost",port);
////            System.out.println("Servernode: " + n.getNodeHostPort());
////            boolean success = serverManager.addKVServer(n,"something", 100);
////            System.out.println(success);
////        }
////        System.in.read();
//
//        String name = "TEST_SERVER_" + Integer.toString(0);
//        ServerNode n = new ServerNode(name,"localhost",1111+0);
//        n.setRange(new BigInteger(String.valueOf(1111233)), new BigInteger(String.valueOf(11111111)));
//        serverManager.zookeeperECSManager.addKVServer(n);
    }

}
