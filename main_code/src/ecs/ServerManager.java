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
import com.jcraft.jsch.*;



public class ServerManager {


//    // hash values (start) -> serverNode
//    private TreeMap<String,ServerNode> tree = new TreeMap<String,ServerNode>();

    // name: (serverName ip) -> serverNode
    private HashMap<String,IECSNode> hashMap = new HashMap<String,IECSNode>(); // stores the active running nodes
    private LinkedList<ConfigEntity> entityList = new LinkedList<ConfigEntity>();
    private ZookeeperECSManager zookeeperManager;
    private static Logger LOGGER = Logger.getLogger(ServerManager.class);
    private Metadata metadataManager = new Metadata();
    private static final String RELATIVE_CONFIG_FILE_PATH = "/src/app_kvECS/ecs.config";
    private static final String privateKeyPath = "/nfs/ug/homes-5/x/xushuran/Desktop/java_test_files/id_rsa";
    private static final String knownHostPath = "~/.ssh/known_hosts";
    private int TIMEOUT = 5000;


    public ServerManager(){
        try {

            zookeeperManager = new ZookeeperECSManager("localhost:2181",10000); // session timeout ms
            System.out.println("Working Directory = " +
                    System.getProperty("user.dir"));
            // start parseConfigFile with path to file
            String filePath = System.getProperty("user.dir") + RELATIVE_CONFIG_FILE_PATH;
            parseConfigFile(filePath);
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
    private static void readChannelOutput(Channel channel){

        byte[] buffer = new byte[1024];

        try{
            InputStream in = channel.getInputStream();
            String line = "";
            while (true){
                while (in.available() > 0) {
                    int i = in.read(buffer, 0, 1024);
                    if (i < 0) {
                        break;
                    }
                    line = new String(buffer, 0, i);
                    System.out.println(line);
                }

                if(line.contains("logout")){
                    break;
                }

                if (channel.isClosed()){
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee){}
            }
        }catch(Exception e){
            System.out.println("Error while reading channel output: "+ e);
        }
    }


    public void remoteLaunchServer(int portNum){

        JSch ssh = new JSch();
        String username;
        username = "xushuran";
        String host = "128.100.13.174";
        //String command = "java -jar /nfs/ug/homes-5/x/xushuran/Desktop/java_test_files/HelloWorld.jar";
        String command = "echo Yo";
        Session session;

        try{
            ssh.setKnownHosts(knownHostPath);
            session = ssh.getSession(username,host,portNum);
            ssh.addIdentity(privateKeyPath);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect(TIMEOUT);
            System.out.println("Connected to " + username + "@" + host + ":" + portNum);


            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);
            InputStream in = channel.getInputStream();
            channel.connect();

            readChannelOutput(channel);
            System.out.println("Connection is closed");
            channel.disconnect();
            session.disconnect();

        }catch(JSchException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public ServerNode addNode(int cacheSize, String cacheStrategy){

        ServerNode node = new ServerNode(entityList.removeFirst(), cacheSize, cacheStrategy);
        try {
            addServer(node, cacheStrategy, cacheSize);
            return node;
        }catch (KeeperException | InterruptedException e){
            e.printStackTrace();
            return null;
        }
    }

    private ServerNode updateHashMapElement(String id, BigInteger[] newRange){

        ServerNode node = null;
        for (String key : hashMap.keySet()) {
            node = (ServerNode) hashMap.get(key);
            String nodeID = node.getNodeHost() + ":" + node.getNodePort();
            if(nodeID.equals(id)){
                node.setRange(newRange[0],newRange[1]);
                hashMap.put(id, node);
                break;
            }
        }
        return node;
    }

    private boolean updateSuccessor(String serverNodeID, ServerNode n){

        BigInteger[] range = metadataManager.findHashRange(serverNodeID);
        if(range != null) {
            n.setRange(range[0], range[1]);
            BigInteger[] successorRange = metadataManager.getNewSuccessorRange(serverNodeID);
            if (successorRange != null) {
                String successorID = metadataManager.getSuccessor(serverNodeID);
                if (successorID != null) {
                    ServerNode updatedNode = updateHashMapElement(successorID, successorRange);
                    try {
                        zookeeperManager.updateRangeKVServer(updatedNode);
                    }catch (KeeperException | InterruptedException e){
                        e.printStackTrace();
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }

    private boolean addServer(ServerNode n, String cacheStrategy, int cacheSize) throws KeeperException, InterruptedException { // change to throw?

        String id = n.getNodeName();
        if (hashMap.containsKey(id)) {
            return false;
        }
        hashMap.put(id,n);
        String serverNodeID = n.getNodeHost() + ":" + Integer.toString(n.getNodePort());
        metadataManager.addServer(serverNodeID);
        zookeeperManager.addKVServer(n);
        return updateSuccessor(serverNodeID, n);

    }

    public void updateMetaDataZNode(){

        //TODO: need API
       //zookeeperManager.addMetaData();

    }

    public IECSNode getServerName(String Key){

        String[] temp = metadataManager.findServer(Key).split(":");
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

        for (String key : hashMap.keySet()) {
            ServerNode node = (ServerNode) hashMap.get(key);
            try {
                zookeeperManager.startKVServer(node);
            }catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public boolean stop(){
        for (String key : hashMap.keySet()) {
            ServerNode node = (ServerNode) hashMap.get(key);
            try {
                zookeeperManager.stopKVServer(node);
            }catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public boolean shutdown(){
        for (String key : hashMap.keySet()) {
            ServerNode node = (ServerNode) hashMap.get(key);
            try {
                zookeeperManager.shutdownKVServer(node);
            }catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    //ServerIndex: NodeName
    public boolean removeNode(String ServerIndex)throws KeeperException, InterruptedException{

        IECSNode node = hashMap.get(ServerIndex);
        //if remove node, add the node back to entity list for next launch
        ConfigEntity entity = new ConfigEntity(node.getNodeHost(), node.getNodeHost(), node.getNodePort());
        entityList.add(entity);
        if (hashMap.containsKey(ServerIndex)){
            metadataManager.removeServer(node.getNodeHost() + " : " + Integer.toString(node.getNodePort()));
            hashMap.remove(ServerIndex);

            String serverNodeID = node.getNodeHost() + ":" + Integer.toString(node.getNodePort());
            if(updateSuccessor(serverNodeID, (ServerNode) node)){
                zookeeperManager.removeKVServer(ServerIndex);
                return true;
            }
            else
                return false;
        }
        return false;
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
           // boolean success = serverManager.addNode(n,"something", 100);
           // System.out.println(success);
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
