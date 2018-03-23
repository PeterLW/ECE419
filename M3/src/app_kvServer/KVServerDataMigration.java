package app_kvServer;

import common.cache.StorageManager;
import common.messages.KVMessage;
import common.messages.Message;
import common.transmission.Transmission;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.google.gson.Gson;

import java.io.IOException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.ArrayList;
import ecs.ServerNode;

public class KVServerDataMigration implements Runnable {

    private  String address;
    private int port;
    private BigInteger[] hashRange;
    private ServerNode serverNode;
    private Transmission transmission;
    StorageManager storageManager;
    private final Gson gson = new Gson();
    private final static Logger LOGGER = Logger.getLogger(KVServerDataMigration.class);
    private final static int MIGRATION_PORT_OFFSET = 50;

    public KVServerDataMigration(ServerNode node, StorageManager storageManager){
        this.serverNode = node;
        this.storageManager = storageManager;
    }

    //Q: where is the kill handler ? (i.e. what if the main thread sends a kill signal here , does
    //cathy use this class as an object or run this thread at startup ? Need a mechanism to kill and close this thread.
    @Override
    public void run() {
        System.out.println("KVServerDataMigration thread starts ....\n");
        while(true){
            ServerStatusType statusType = serverNode.getServerStatus().getStatus();
            if ((statusType == ServerStatusType.MOVE_DATA_RECEIVER || statusType == ServerStatusType.MOVE_DATA_SENDER) && (serverNode.getServerStatus().isReady() == false)){ 
                update(statusType);
                start(statusType);
                finish();
                try {
                    Thread.sleep(10);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            } else if (statusType == ServerStatusType.CLOSE){
                break;
            }
            else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void print_servernode(){

        System.out.println("servernode.name = " + serverNode.getName());
        System.out.println("servernode.host = " + serverNode.getNodeHost());
        System.out.println("servernode.port = " + Integer.toString(serverNode.getNodePort()));
        System.out.println("servernode.status.targetName = " + serverNode.getServerStatus().getTargetName());
        System.out.println("servernode.status.getStatus = " + serverNode.getServerStatus().getStatus());
    }


private void print_stats(String localName, String targetName){

    System.out.println("servernode.localName = " + localName);
    System.out.println("servernode.targetName = " + targetName);
    System.out.println("servernode.address = " + address);
    System.out.println("servernode.port = " + port);
    System.out.println("servernode.hashRange = " + hashRange);

}
    private void update(ServerStatusType statusType) {
       
        String targetName = null;
        String localName = null;

        if(statusType == ServerStatusType.MOVE_DATA_RECEIVER){
            
            localName = serverNode.getNodeHostPort();
            System.out.println("the receiver is " + localName);
            System.out.println("the sender is " + serverNode.getServerStatus().getTargetName());
            String[] temp = localName.split(":");
            this.address = temp[0];
            this.port = Integer.parseInt(temp[1]) + MIGRATION_PORT_OFFSET;
        }
        else{
           
            localName = serverNode.getNodeHostPort();
            System.out.println("the sender is " + localName);
            targetName = serverNode.getServerStatus().getTargetName();
            System.out.println("the receiver is " + targetName);

            String[] temp = targetName.split(":");
            this.address = temp[0];
            this.port = Integer.parseInt(temp[1]) + MIGRATION_PORT_OFFSET;
        }
        
        this.hashRange = serverNode.getServerStatus().getMoveRange();
        this.transmission = new Transmission();

        print_stats(localName,targetName);
    }

    private void start(ServerStatusType statusType) {

        if (statusType == ServerStatusType.MOVE_DATA_SENDER) {

            try {
                Socket senderSocket = new Socket(address, port);
                System.out.println("connecting to receiver...");
                send_data(senderSocket);
                senderSocket.close();
            } catch (IOException e) {
                LOGGER.error("Failed to connect data migration receiver");
                return;
            }
        }
        else if (statusType == ServerStatusType.MOVE_DATA_RECEIVER) {
            try {
                System.out.println("Data migration created a ServerSocket: " + Integer.toString(port));
                ServerSocket receiverSocket = new ServerSocket(port);
                
                Socket socket = receiverSocket.accept();
                System.out.println("Connected to the sender, start receiving packets\n");
                receive_data(socket);
                socket.close();
                receiverSocket.close();
                System.out.println("Socket closed\n");
            } catch (IOException e) {
                LOGGER.error("Failed to connect data migration sender");
                return;
            }
        }
        else{
            System.out.println("something is wrong...\n");
        }
    }

    private void finish() {

        System.out.println("finish");
        if(serverNode.getServerStatus().getRangeUpdate()) {

            serverNode.setRange(serverNode.getServerStatus().getCoordRange());
            serverNode.setReplicaOneRange(serverNode.getServerStatus().getReplicaOneRange());
            serverNode.setReplicaTwoRange(serverNode.getServerStatus().getReplicaTwoRange());
        }
        //TODO: might need to do something here, not sure now ...
        serverNode.getServerStatus().setReady();
        System.out.println(serverNode.getNodeHostPort() + ": data migration finishes\n");
    }

    public void send_data(Socket senderSocket) throws IOException {
       
       if(hashRange == null){
            Message message = new Message(KVMessage.StatusType.CLOSE_REQ, 0, 0, null, null);
            transmission.sendMessage(toByteArray(gson.toJson(message)), senderSocket);
            return;
       }
        ArrayList<String> keys = storageManager.returnKeysInRange(hashRange);
        if(keys.isEmpty()){
            System.out.println("Warning: No keys to be sent\n");
        }
        for (String key : keys) {
            String value = storageManager.getKV(key);
            Message message = new Message(KVMessage.StatusType.PUT, 0, 0, key, value);
            transmission.sendMessage(toByteArray(gson.toJson(message)), senderSocket);

            Message recvMsg = transmission.receiveMessage(senderSocket);
            if (recvMsg.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
                if(serverNode.getServerStatus().getLocalRemove()) {
                    storageManager.deleteRV(key); //we delete data both on cache and disk in M3, and we only delete on cache on M2.
                }
                System.out.println(recvMsg.getStatus().toString());
            } else {
                //todo... possibly retry and send error message
                LOGGER.error("Sender: failed to migrate a packet");
            }
        }
        Message message = new Message(KVMessage.StatusType.CLOSE_REQ, 0, 0, null, null);
        transmission.sendMessage(toByteArray(gson.toJson(message)), senderSocket);
    }

    public void receive_data(Socket receiverSocket) throws IOException{

        Message data = transmission.receiveMessage(receiverSocket);
        while(!(data.getStatus() == KVMessage.StatusType.CLOSE_REQ)) {
            if(data.getStatus() == KVMessage.StatusType.PUT) {

                System.out.println("Packets (" + data.getKey() +"," + data.getValue() + ") " + "received\n");
                storageManager.putKV(data.getKey(), data.getValue());

                Message retMessage = new Message(KVMessage.StatusType.PUT_SUCCESS, 0, 0, null, null);
                System.out.println("PUT_SUCCESS: sent ack to the sender");
                transmission.sendMessage(toByteArray(gson.toJson(retMessage)), receiverSocket);

                data = transmission.receiveMessage(receiverSocket);
            }
            else{
                System.out.println("Receiver: Wrong data migration status");
                break;
            }
        }
        System.out.println("CLOSE_REQ: finished receiving packets\n");
    }

    //generic function, better move to a common file as well as getMD5() function
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    private byte[] toByteArray(String s){
        byte[] bytes = s.getBytes();
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }


//    public static void main(String[] args){
//
//        try {
//            new LogSetup("logs/server.log", Level.DEBUG);
//        } catch (IOException e) {
//            System.out.println("Error! Unable to initialize logger!");
//            e.printStackTrace();
//            System.exit(1);
//        }
//
//        StorageManager sm = new StorageManager(1000, "FIFO");
//        sm.clearAll();
//        sm.putKV("a", "a");
//        sm.putKV("b", "b");
//        sm.putKV("c", "c");
//        sm.putKV("d", "d");
//
//        BigInteger[] hashRange = new BigInteger[2];
//        try {
//            hashRange[0] = getMD5("0");
//            hashRange[1] = getMD5("k");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        ServerStatus status = new ServerStatus(hashRange, "192.168.1.132:50000", ServerStatusType.MOVE_DATA_SENDER);
//        ServerNode serverNode = new ServerNode("server", "192.168.1.132", 50000);
//        serverNode.setServerStatus(status);
//
//        KVServerDataMigration dm = new KVServerDataMigration("192.168.1.132:50000", hashRange, serverNode, sm);
//        dm.run();
//
//    }
//
//    public static BigInteger getMD5(String input) throws Exception{
//
//        MessageDigest md=MessageDigest.getInstance("MD5");
//        md.update(input.getBytes(),0,input.length());
//        String hash_temp = new BigInteger(1,md.digest()).toString(16);
//        BigInteger hash = new BigInteger(hash_temp, 16);
//        return hash;
//    }


}
