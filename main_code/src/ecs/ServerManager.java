package ecs;


import app_kvServer.ServerStatus;
import common.Metadata.Metadata;
import common.zookeeper.ZookeeperECSManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public class ServerManager {
//    // hash values (start) -> serverNode
//    private TreeMap<String,ServerNode> tree = new TreeMap<String,ServerNode>();

    // name: (serverName ip) -> serverNode
    private HashMap<String,IECSNode> hashMap = new HashMap<String,IECSNode>(); // stores the active running nodes
    private LinkedList<ConfigEntity> entityList = new LinkedList<ConfigEntity>();
    private ZookeeperECSManager zookeeperManager;
    private static Logger LOGGER = Logger.getLogger(ServerManager.class);
    private Metadata metadataManager = new Metadata();
    private static final String CONFIG_FILE_PATH = "ecs.config";

    public ServerManager(){
        try {
            zookeeperManager = new ZookeeperECSManager("localhost:2181",10000); // session timeout ms
            // start parseConfigFile with path to file
            parseConfigFile(CONFIG_FILE_PATH);
        } catch (Exception e) {
            LOGGER.error("Failed to connect to zookeeper. Check that zookeeper server has started and is running on localhost:2181");
            throw new RuntimeException("Failed to connect to zookeeper. Check that zookeeper server has started and is running on localhost:2181", e);
        }
    }

    public HashMap<String,IECSNode> getServerMap(){
        return hashMap;
    }

    public int getNumOfServerConnected(){

        //TODO: need to finish the accept( ) first.
        return 0;
    }
    //this function will do the ssh thing.
    private void remoteLaunchServer(int portNum){


    }

    public ServerNode addNode(int cacheSize, String cacheStrategy){

        ServerNode node = new ServerNode(entityList.removeFirst(), cacheSize, cacheStrategy);
        try {
            addNode(node, cacheStrategy, cacheSize);
            return node;
        }catch (KeeperException | InterruptedException e){
            e.printStackTrace();
            return null;
        }
    }

    public boolean addNode(ServerNode n, String cacheStrategy, int cacheSize) throws KeeperException, InterruptedException { // change to throw?
        String id = n.getNodeName();
        if (hashMap.containsKey(id)) {
            return false;
        }
        zookeeperManager.addKVServer(n);
        hashMap.put(id,n);
        metadataManager.add_server(n.getNodeHost() + " : " + Integer.toString(n.getNodePort()));
        remoteLaunchServer(n.getNodePort());
        return true;
    }

    public IECSNode getServerName(String Key){

        String[] temp = metadataManager.find_server(Key).split(":");
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
//        for (Map.Entry<String,ServerNode> entry : hashMap.)
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

    //ServerIndex: NodeName
    public void removeNode(String ServerIndex)throws KeeperException, InterruptedException{

        IECSNode node = hashMap.get(ServerIndex);
        zookeeperManager.removeKVServer(ServerIndex);
        ConfigEntity entity = new ConfigEntity(node.getNodeHost(), node.getNodeHost(), node.getNodePort());
        entityList.add(entity);
        if (hashMap.containsKey(ServerIndex)){
            metadataManager.remove_server(node.getNodeHost() + " : "+ Integer.toString(node.getNodePort()));
            hashMap.remove(ServerIndex);

        } else {
            LOGGER.debug("Trying to remove server: " + ServerIndex + " but server not in hash map");
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
            e.printStackTrace();
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
