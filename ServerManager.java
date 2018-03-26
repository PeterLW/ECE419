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
    private static final String ZOOKEEPER_HOST_NAME = "localhost";
    private static final String ZOOKEEPER_PORT = "2191";
    public static final String ZOOKEEPER_HOST_PORT = ZOOKEEPER_HOST_NAME + ":" + ZOOKEEPER_PORT;

    // TODO: make these thread-safe with Colletions.synchronizedList(new Object<>())
    private LinkedHashMap<String,IECSNode> hashMap = new LinkedHashMap<String,IECSNode>(); // stores the active running nodes
    private LinkedList<ConfigEntity> entityList = new LinkedList<ConfigEntity>(); // stores provided nodes from Config file
    private LinkedList<ConfigEntity> originalEntityList = new LinkedList<ConfigEntity>();

    private ZookeeperECSManager zookeeperECSManager;
    private boolean is_init;
    private static final int SLEEP_TIME = 1000;
    private ServerNode successorNode;
    private BigInteger[] successorRange;
    private String successorPort;


    public ServerManager(){
        try {
            zookeeperECSManager = new ZookeeperECSManager(ZOOKEEPER_HOST_PORT,10000); // session timeout ms
            System.out.println("Working Directory = " + System.getProperty("user.dir"));
            // start parseConfigFile with path to file
            String filePath =new File(".").getCanonicalPath()+RELATIVE_CONFIG_FILE_PATH;
            parseConfigFile(filePath);
        } catch (Exception e) {
            LOGGER.error("Failed to connect to zookeeper. Check that zookeeper server has started and is running on " + ZOOKEEPER_PORT );
            throw new RuntimeException("Failed to connect to zookeeper. Check that zookeeper server has started and is running on localhost: " + ZOOKEEPER_PORT, e);
        }
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
                    //System.out.println("node.serverName = "+node.getNodeName() + ", node.ipport = " +node.getNodeHostPort());

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
                    BigInteger[] newRange = metadata.findHashRange(node.getNodeHostPort());
                    node.setRange(newRange);
                    list.add(node);

                    zookeeperECSManager.updateMetadataZNode(metadata); // update metadata node
                    
                    //System.out.println("node.serverName = "+node.getNodeName() + ", node.ipport = " +node.getNodeHostPort());
                    this.addServer(node, cacheStrategy, cacheSize);
                    SSH ssh_conn = new SSH(node.getNodeHostPort(), ZOOKEEPER_HOST_NAME,ZOOKEEPER_PORT);
                    Thread sshConnThread = new Thread(ssh_conn);
                    sshConnThread.start();
                    //Assume the delay enables the new KVServer thread to be launched
                    try{
                        Thread.sleep(SLEEP_TIME);
                    }catch(InterruptedException e){
                        e.printStackTrace();
                    }

                    String successorID = metadata.getSuccessor(node.getNodeHostPort());
                    ServerNode successor = getServerNode(successorID);
                    updateServerStatusFlags(successor, false, false); //B doesn't need to remove the data, since it is its replicaRangeTwo
                    zookeeperECSManager.moveDataSenderKVServer(successor, newRange, node.getNodeHostPort());
                    Thread.sleep(SLEEP_TIME);
                    dataReplicationForNewKVServer(hashMap.size(),node, successor, newRange);

                    //zookeeperECSManager.moveDataSenderKVServer(this.successorNode, this.successorRange, node.getNodeHostPort());

                }
            }
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error("Failed to update metadata");
            e.printStackTrace();
            return null;
        }
        return list;

    }

    //the metadata is updated by now, so the structure to be used here is metadata.
    //here we update the server node stored in hash map.

    private void updateReplicaRanges(String nodeID){

        String predecessorID = metadata.getPredecessor(nodeID);
        BigInteger[] newCoordRange = metadata.findHashRange(nodeID);
        BigInteger[] newReplicaOneRange = null;
        BigInteger[] newReplicaTwoRange = null;

        if(predecessorID != null) {

            newReplicaTwoRange = metadata.findHashRange(predecessorID);
            String prepPredecessorID = metadata.getPredecessor(nodeID);

            if (prepPredecessorID != null && !predecessorID.equals(nodeID)) { //when there are only 2 nodes in the ring, this happens
                newReplicaOneRange = metadata.findHashRange(prepPredecessorID);
            }
        }
        setHashMapNodeRanges(nodeID, newCoordRange, newReplicaOneRange, newReplicaTwoRange);
    }

    private void setHashMapNodeRanges(String id,BigInteger[] newcoordRange, BigInteger[] newReplicaOneRange, BigInteger[] newReplicaTwoRange){ // renamed fct didn't change code
        ServerNode node = null;

        for (Map.Entry<String, IECSNode> entry : hashMap.entrySet()) {
            node = (ServerNode) entry.getValue();
            if(node.getNodeHostPort().equals(id)){
                node.setRange(newcoordRange[0],newcoordRange[1]); // this line change the node instance in hash map,
                node.setReplicaTwoRange(newReplicaTwoRange);
                node.setReplicaOneRange(newReplicaOneRange);
                // don't need to put it back
                // hashMap.put(id, node);
                break;
            }
        }
    }

    private ServerNode getServerNode(String id){

        if(id == null){
            return null;
        }
        ServerNode node = null;
        for (Map.Entry<String, IECSNode> entry : hashMap.entrySet()) {
            node = (ServerNode) entry.getValue();
            if(node.getNodeHostPort().equals(id)){
                break;
            }
        }
        return node;
    }

    /**
     * add server into correct data structures
     *
     * Update all ranges after the data migration so that clients can't access data that has not been arrived
     */

    private void updateServerStatusFlags(ServerNode n, boolean removeLocal, boolean updateRange){

        n.getServerStatus().setLocalRemove(removeLocal);
        n.getServerStatus().setRangeUpdate(updateRange);
        if(updateRange){
            n.getServerStatus().setNewRanges(n.getRange(), n.getReplicaOneRange(), n.getReplicaTwoRange());
        }
    }

    private boolean isBelongingToSuccessors(String nodeID, String checkID){

        String successorID = metadata.getSuccessor(nodeID);
        if(successorID == null){
            return false;
        }
        else{
            if(successorID.equals(checkID)){
                return true;
            }
            String nextSuccessorID = metadata.getSuccessor(successorID);
            if(nextSuccessorID == null){
                return false;
            }
            else{
                if(nextSuccessorID.equals(checkID)){
                    return true;
                }
            }
        }
        return false;
    }

    private boolean removeReplicaDataOrNot(String currNodeID, BigInteger[] range){

        if(range == null){
            return false;
        }
        ServerNode node = null;
        for (Map.Entry<String, IECSNode> entry : hashMap.entrySet()) {
            node = (ServerNode) entry.getValue();
            BigInteger[] nodeRange = node.getRange();
            if(nodeRange == null){ //in this case, possibly only one node is active in the ring, then don't remove it
                continue;
            }
            else{
                if((nodeRange[0].compareTo(range[0]) == 0) &&
                        (nodeRange[1].compareTo(range[1])) == 0){
                        //find the nodeID
                        String checkNodeID = node.getNodeHostPort();
                        if(isBelongingToSuccessors(currNodeID,checkNodeID)){
                            return false;
                        }
                        else{
                            return true;
                        }
                }
            }
        }
        //if reached here, it means that something is wrong, since even when there is only one node in the ring, all values
        //belong to that node
        System.out.println("removeReplicaDataOrNot( ): program reached here, something is wrong !\n");
        return false; //at this time we don't do deletion just for safety concern
    }

    private void dataReplicationForNewKVServer(int numNodes, ServerNode n, ServerNode successor, BigInteger[] newRange) throws KeeperException, InterruptedException{

        BigInteger[] MoveRange;
        if(numNodes > 2) {

            String predecessorID = metadata.getPredecessor(n.getNodeHostPort());

            //in my note example, this is A --> D for (F,A), which is RR2 for new node
            if (predecessorID != null) {

                ServerNode predecessor = getServerNode(predecessorID);
                MoveRange = predecessor.getRange();
                updateServerStatusFlags(n, false, true);
                zookeeperECSManager.moveDataReceiverKVServer(n, MoveRange, predecessorID);
                Thread.sleep(SLEEP_TIME);
                updateServerStatusFlags(predecessor, false, true);
                zookeeperECSManager.moveDataSenderKVServer(predecessor, MoveRange, n.getNodeHostPort());
                Thread.sleep(SLEEP_TIME);
            }

            //Note: here we can assume that the prePredeccesor exists, since there are > 2 servers in the ring, this assumption is valid.
            //in my note example, this is F --> D for (E,F), which is the RR1 for new node
            String prePredecessorID = metadata.getPredecessor(predecessorID);
            ServerNode prePredeccesor = getServerNode(prePredecessorID);
            MoveRange = prePredeccesor.getRange();
            updateServerStatusFlags(n, false, false);
            zookeeperECSManager.moveDataReceiverKVServer(n, MoveRange, prePredecessorID);
            Thread.sleep(SLEEP_TIME);
            updateServerStatusFlags(prePredeccesor, false, true);
            zookeeperECSManager.moveDataSenderKVServer(prePredeccesor, MoveRange, n.getNodeHostPort());
            Thread.sleep(SLEEP_TIME);

            //in my note example, E needs to remove (A,D), as it is no longer part of its replicaOneRange.
            //However, it is not necessary to do the deletion. Here we skip the deletion as it is harmless
            //to the design.
        }
        else{
            /*
            When only nodes in the ring, let the successor send its coordinate range to the new node as the replicaTwoRange
             */
            MoveRange = successor.getRange();
            updateServerStatusFlags(n, false, false);
            zookeeperECSManager.moveDataReceiverKVServer(n, MoveRange, successor.getNodeHostPort());
            Thread.sleep(SLEEP_TIME);
            updateServerStatusFlags(successor, false, true);
            zookeeperECSManager.moveDataSenderKVServer(successor, MoveRange, n.getNodeHostPort());
            Thread.sleep(SLEEP_TIME);
        }

    }

    private boolean addServer(ServerNode n, String cacheStrategy, int cacheSize) throws KeeperException, InterruptedException { // change to throw?

        String nodeID = n.getNodeHostPort();
        if (hashMap.containsKey(nodeID)) {
            return false;
        }

        BigInteger[] newRange = metadata.findHashRange(nodeID);
        if(newRange == null){
            System.out.println("addServer: failed to find the hash range for "+ nodeID);
            return false;
        }
        else{
            System.out.println("addServer : " +nodeID + " hash range = (" + newRange[0].toString() + "," + newRange[1].toString() +")");
        }
        n.setRange(newRange);
        hashMap.put(nodeID,n); //here we update the coordinate range for the new node

        try {
            //set replica ranges for the new node, this shall be the last step
            updateReplicaRanges(nodeID);

            int mapSize = hashMap.size();
            if(mapSize >= 2) {

                //set replica ranges for all possible three successor nodes
                String successorID = metadata.getSuccessor(nodeID);
                if(successorID != null) {
                    updateAllThreeSuccessorsRanges(successorID);
                }
                else{
                    System.out.println("addServer( ): error, successor is null for the ring containing more than 2 nodes !\n");
                }
            }

            if(!is_init) {

                String successorID = metadata.getSuccessor(nodeID);
                ServerNode successor = getServerNode(successorID);

                if(successor != null) {

                    //now it's time for ECS manager to send out commands
                    //in my note example, this is B --> D for (A,D)
                    updateServerStatusFlags(n, false, false);
                    zookeeperECSManager.addAndRecvDataKVServer(n, newRange, successor.getNodeHostPort());
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

    private void updateAllThreeSuccessorsRanges(String successorID){

        String nextSuccessorID = null;
        String nextNextSuccessorID = null;
        //update the 1st successor's coordinate range in hash map's server node
        updateReplicaRanges(successorID);
        //update the 2nd successor's coordinate range in hash map's server node
        nextSuccessorID = metadata.getSuccessor(successorID);
        if(nextSuccessorID != null) {
            updateReplicaRanges(nextSuccessorID);
            //update the 3rd successor
            nextNextSuccessorID = metadata.getSuccessor(nextSuccessorID);
            if(nextNextSuccessorID != null) {
                updateReplicaRanges(nextNextSuccessorID);
            }
            else{
                System.out.println("updateAllThreeSuccessorsRanges( ): There are two successors\n");
            }
        }
        else{
            System.out.println("updateAllThreeSuccessorsRanges( ): There is one successor\n");
        }
    }

    public String removeNode(Integer serverIndex) {
        ConfigEntity configEntity = originalEntityList.get(serverIndex);
        String serverIpPort = configEntity.getIpAddr() + ":" + configEntity.getPortNum(); // actually serverIpPort
        System.out.println("Attempting to remove server node: " + serverIpPort);
        return removeNode(serverIpPort, false);
    }


    //In removeNode( ): the order of replica range updates and ECS message passing is irrevelant !
    public String removeNode(String ServerIndex, boolean recovery) {

        if (hashMap.containsKey(ServerIndex)){
            ServerNode node = (ServerNode) hashMap.get(ServerIndex);
            //if remove node, add the node back to entity list for next launch
            ConfigEntity entity = new ConfigEntity(node.getNodeHost(), node.getNodeHost(), node.getNodePort());
            entityList.add(entity);

            //get the range needed to be sent from the deleted node to the successor node
            BigInteger[] range = metadata.findHashRange(ServerIndex);
            String nodeID = node.getNodeHostPort();
            String successorID = metadata.getSuccessor(nodeID);
            int cacheSize = node.getCacheSize();
            String cacheStrategy = node.getCacheStrategy();

            //remove the node from the ring
            metadata.removeServer(nodeID);
            //remove the node from the hashMap
            hashMap.remove(ServerIndex);

            //now update all ranges associated with possible all three successors for the deleted node.
            if(successorID != null) {
                updateAllThreeSuccessorsRanges(successorID);
            }
            else{
                System.out.println("removeNode( ): There is no successor for node " + ServerIndex + "\n");
            }

            try {
                if (!hashMap.isEmpty()) {
                    //first get predecessor and successor
                    String predecessorID = metadata.getPredecessor(successorID);
                    String prePredecessorID = metadata.getPredecessor(predecessorID);
                    String nextSuccessorID = metadata.getSuccessor(successorID);
                    String nextNextSuccessorID = metadata.getSuccessor(nextSuccessorID);

                    ServerNode successor = getServerNode(successorID);

                    //For the successor's new CoordinateRange:
                    updateServerStatusFlags(successor, false, false);
                    zookeeperECSManager.moveDataReceiverKVServer(successor, range, nodeID);
                    Thread.sleep(SLEEP_TIME);
                    updateServerStatusFlags(node, true, true);
                    zookeeperECSManager.removeAndMoveDataKVServer(node, range, successorID);
                    Thread.sleep(SLEEP_TIME);

                    if(predecessorID != null){

                        //For the successor's ReplicaRangeTwo
                        ServerNode predecessor = getServerNode(predecessorID);
                        BigInteger[] replicaTwoMoveRange = predecessor.getRange();
                        updateServerStatusFlags(successor, false, false);
                        zookeeperECSManager.moveDataReceiverKVServer(successor, replicaTwoMoveRange, predecessorID);

                        Thread.sleep(SLEEP_TIME);

                        //set the localRemove flag on predecessor to false so that the transferred data will not be removed
                        updateServerStatusFlags(predecessor, false, false);
                        zookeeperECSManager.moveDataSenderKVServer(predecessor, replicaTwoMoveRange, successorID);
                        Thread.sleep(SLEEP_TIME);


                        //For the successor's ReplicaRangeOne
                        if(prePredecessorID != null && !prePredecessorID.equals(successorID)){
                            ServerNode prePredecessor = getServerNode(predecessorID);
                            BigInteger[] replicaRangeOne = prePredecessor.getRange();

                            updateServerStatusFlags(successor, false, true);
                            zookeeperECSManager.moveDataReceiverKVServer(successor, replicaRangeOne, prePredecessorID);
                            Thread.sleep(SLEEP_TIME);

                            //set the localRemove flag on predecessor to false so that the transferred data will not be removed
                            updateServerStatusFlags(prePredecessor, false, false);
                            zookeeperECSManager.moveDataSenderKVServer(prePredecessor, replicaRangeOne, successorID);
                            Thread.sleep(SLEEP_TIME);
                        }

                        //For the nextSuccessor's new replicaRangeTwo:
                        if(nextSuccessorID != null) {

                            ServerNode nextSuccessor = getServerNode(nextSuccessorID);
                            BigInteger[] replicaRangeTwo = successor.getRange();
                            updateServerStatusFlags(nextSuccessor, false, true);
                            zookeeperECSManager.moveDataReceiverKVServer(nextSuccessor, replicaRangeTwo, successorID);
                            Thread.sleep(SLEEP_TIME);

                            updateServerStatusFlags(successor, false, false);
                            zookeeperECSManager.moveDataSenderKVServer(successor, replicaRangeTwo, nextSuccessorID);
                            Thread.sleep(SLEEP_TIME);

                            //For the nextSuccessor's new replicaRangeOne:
                            if(!predecessorID.equals(nextSuccessorID)) {
                                BigInteger[] replicaRangeOne = predecessor.getRange();
                                updateServerStatusFlags(nextSuccessor, false, true);
                                zookeeperECSManager.moveDataReceiverKVServer(nextSuccessor, replicaRangeOne, predecessorID);
                                Thread.sleep(SLEEP_TIME);

                                updateServerStatusFlags(predecessor, false, false);
                                zookeeperECSManager.moveDataSenderKVServer(predecessor, replicaRangeOne, nextSuccessorID);
                                Thread.sleep(SLEEP_TIME);
                            }

                        }
                        //For the nextNextSuccessor's
                        if(nextNextSuccessorID != null && !nextNextSuccessorID.equals(successorID)){

                            ServerNode nextNextSuccessor = getServerNode(nextNextSuccessorID);
                            BigInteger[] replicaOneMoveRange = successor.getRange();
                            updateServerStatusFlags(nextNextSuccessor, false, true);
                            zookeeperECSManager.moveDataReceiverKVServer(nextNextSuccessor, replicaOneMoveRange, successorID);
                            Thread.sleep(SLEEP_TIME);
                            updateServerStatusFlags(successor, false, true);
                            zookeeperECSManager.moveDataSenderKVServer(successor, replicaOneMoveRange, nextNextSuccessorID);
                            Thread.sleep(SLEEP_TIME);
                        }
                }
                else{ //in this case, there is only one node in the ring, we can simply send shutdown message to the deleted node
                    System.out.println("removeNode( ): Now only one node in the ring\n");
                    //Thread.sleep(SLEEP_TIME);
                }
            }
            zookeeperECSManager.shutdownKVServer(node);
            Thread.sleep(SLEEP_TIME);
            if(recovery){
                setupNodes(1, cacheStrategy, cacheSize);
            }
        }catch (KeeperException | InterruptedException e){
            e.printStackTrace();
            return null;
        }
        return ServerIndex;
    }
        else {
            LOGGER.error("Cannot remove non-existing node!");
            return null;
        }
    }


//    public boolean removeNode(String ServerIndex)throws KeeperException, InterruptedException{
//
//        if (hashMap.containsKey(ServerIndex)){
//            ServerNode node = (ServerNode) hashMap.get(ServerIndex);
//            //if remove node, add the node back to entity list for next launch
//            ConfigEntity entity = new ConfigEntity(node.getNodeHost(), node.getNodeHost(), node.getNodePort());
//            entityList.add(entity);
//
//            //get the range needed to be sent from the deleted node to the successor node
//            BigInteger[] range = metadata.findHashRange(ServerIndex);
//            String successorID = metadata.getSuccessor(node.getNodeHostPort());
//            String nextSuccessorID = null;
//            String nextNextSuccessorID = null;
//
//            //remove the node from the ring
//            metadata.removeServer((node).getNodeHostPort());
//            //remove the node from the hashMap
//            hashMap.remove(ServerIndex);
//
//            //now update all ranges associated with possible all three successors for the deleted node.
//            if(successorID != null) {
//                updateAllThreeSuccessorsRanges(successorID);
//            }
//            else{
//                System.out.println("removeNode( ): There is no successor for node " + ServerIndex + "\n");
//            }
//
//            try {
//                if (!hashMap.isEmpty()) {
//
//                    ServerNode successor = getServerNode(successorID);
//                    //migrate the target node's coordinate range to the successor first
//                    //for receive, node.getNodeHostPort( ) just indicates the sender, not being used in Data migration.
//
//                    //In the meeting example, this is B -> C
//                    zookeeperECSManager.moveDataReceiverKVServer(successor, range, node.getNodeHostPort());
//                    Thread.sleep(SLEEP_TIME);
//                    zookeeperECSManager.removeAndMoveDataKVServer(node, range, successor.getNodeHostPort());
//                    Thread.sleep(SLEEP_TIME);
//                    //the target's predecessor sends its replicaOneRange to the successor
//                    String predecessorID = metadata.getPredecessor(successorID);
//
//                    if(predecessorID != null){
//
//                        ServerNode predecessor = getServerNode(predecessorID);
//                        BigInteger[] replicaTwoMoveRange = predecessor.getReplicaTwoRange();
//                        //migrate the predecessor's replicaOneRange to the deleted node's successor
//
//                        //In the meeting example, this is A -> C
//                        updateServerStatusFlags(successor, false, false);
//                        zookeeperECSManager.moveDataReceiverKVServer(successor, replicaTwoMoveRange, predecessor.getNodeHostPort());
//
//                        Thread.sleep(SLEEP_TIME);
//
//                        //set the localRemove flag on predecessor to false so that the transferred data will not be removed
//                        updateServerStatusFlags(predecessor, false, false);
//                        zookeeperECSManager.moveDataSenderKVServer(predecessor, replicaTwoMoveRange, successor.getNodeHostPort());
//                        Thread.sleep(SLEEP_TIME);
//
//                        nextSuccessorID = metadata.getSuccessor(successorID);
//                        if(nextSuccessorID != null){
//
//                            ServerNode nextSuccessor = getServerNode(predecessorID);
//                            BigInteger[] predeceesorRange = predecessor.getRange();
//                            //in the meeting example, this is A -> D
//                            updateServerStatusFlags(nextSuccessor, false, true);
//                            zookeeperECSManager.moveDataReceiverKVServer(nextSuccessor, predeceesorRange, predecessor.getNodeHostPort());
//
//                            Thread.sleep(SLEEP_TIME);
//                            updateServerStatusFlags(predecessor, false, true);
//                            zookeeperECSManager.moveDataSenderKVServer(predecessor, predeceesorRange, nextSuccessor.getNodeHostPort());
//                            Thread.sleep(SLEEP_TIME);
//
//
//                            nextNextSuccessorID = metadata.getSuccessor(nextSuccessorID);
//                            if(nextNextSuccessorID != null){
//
//                                ServerNode nextNextSuccessor = getServerNode(nextNextSuccessorID);
//                                BigInteger[] replicaOneMoveRange = range;
//                                //in the meeting example, this is C -> E
//                                updateServerStatusFlags(nextNextSuccessor, false, true);
//                                zookeeperECSManager.moveDataReceiverKVServer(nextNextSuccessor, replicaOneMoveRange, successor.getNodeHostPort());
//                                Thread.sleep(SLEEP_TIME);
//                                updateServerStatusFlags(successor, false, true);
//                                zookeeperECSManager.moveDataSenderKVServer(successor, replicaOneMoveRange, nextNextSuccessor.getNodeHostPort());
//                                Thread.sleep(SLEEP_TIME);
//
//                            }
//                            else{
//                                System.out.println("removeNode(): there is no 3rd successor\n");
//                            }
//                        }
//                        else{
//                            System.out.println("removeNode(): there is no 2nd successor\n");
//                        }
//                    }
//                    else{ //in this case, there is only one node in the ring, we can simply send shutdown message to the deleted node
//                        System.out.println("removeNode( ): Now only one node in the ring\n");
//                    }
//                } else {
//                    zookeeperECSManager.shutdownKVServer(node);
//                }
//            }catch (KeeperException | InterruptedException e){
//                e.printStackTrace();
//                return false;
//            }
//            return true;
//        }
//        else {
//            LOGGER.error("Cannot remove non-existing node!");
//            return false;
//        }
//    }

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
                originalEntityList.add(node); // this one doesn't get removed from later
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
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
