package app_kvServer;

import common.cache.StorageManager;
import common.KVMessage;
import common.Message;
import common.transmission.Transmission;
import org.apache.log4j.Logger;
import com.google.gson.Gson;

import java.io.IOException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import ecs.ServerNode;

public class KVServerDataMigration implements Runnable {

    private  String address;
    private int port;
    private BigInteger[] hashRange;
    private ServerNode serverNode;
    private Transmission transmission = new Transmission();
    StorageManager storageManager;
    private final Gson gson = new Gson();
    private final static Logger LOGGER = Logger.getLogger(KVServerDataMigration.class);

    public KVServerDataMigration(ServerNode node, StorageManager storageManager){
        this.serverNode = node;
        this.storageManager = storageManager;
    }

    //Q: where is the kill handler ? (i.e. what if the main thread sends a kill signal here , does
    //cathy use this class as an object or run this thread at startup ? Need a mechanism to kill and close this thread.
    @Override
    public void run() {
        System.out.println(serverNode.getNodeHostPort() + " > KVServerDataMigration thread starts ....\n");
        while(true){
            ServerStatusType statusType = serverNode.getServerStatus().getStatus();
            if (statusType == ServerStatusType.MOVE_DATA_RECEIVER || statusType == ServerStatusType.MOVE_DATA_SENDER){ ;
                update();
                start();
                finish();
                try {
                    Thread.sleep(10);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            } else if (statusType == ServerStatusType.CLOSE){
                break;
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void datamigration_v2(){
        storageManager.clearCache();
    }

    private void update(){
        String targetName = serverNode.getServerStatus().getTargetName();
        String[] temp = targetName.split(":");

        ServerStatusType currStatus = serverNode.getServerStatus().getStatus();

        if (currStatus == ServerStatusType.MOVE_DATA_SENDER) { // we using the receiving KVSERVER's (port + 1) by default
            this.address = temp[0];
            this.port = Integer.parseInt(temp[1]) + 1;
        } else {
            this.address = "localhost";
            this.port = serverNode.getNodePort() + 1; // my port + 1
        }

        this.hashRange = serverNode.getServerStatus().getMoveRange();
    }

    private void start() {
        if (serverNode.getServerStatus().getStatus() == ServerStatusType.MOVE_DATA_SENDER) {
            System.out.println(serverNode.getNodeHostPort() + " > is trying connecting to receiver: " + serverNode.getServerStatus().getTargetName());
            System.out.println(serverNode.getNodeHostPort() + " > trying to connect on: " + address + ":" + port);

            while(true) { // keep trying to connect
                try {
                    Socket senderSocket = new Socket(address, port);
                    System.out.println(serverNode.getNodeHostPort() + " > is connected to receiver: " + address + ":" + port);
                    send_data(senderSocket);
                    senderSocket.close();
                    break;
                } catch (IOException e) {
                    LOGGER.error("Failed to connect data migration receiver");
                }
            }
        } else if (serverNode.getServerStatus().getStatus() == ServerStatusType.MOVE_DATA_RECEIVER) {
            System.out.println(serverNode.getNodeHostPort() + " > is the receiver, waiting on port ("
                    + port + ") for sender: " + serverNode.getServerStatus().getTargetName());
            try {
                ServerSocket receiverSocket = new ServerSocket(port);
                Socket socket = receiverSocket.accept(); // blocking
                System.out.println(serverNode.getNodeHostPort() + " connected " + socket.getLocalAddress() + " " + socket.getLocalPort());
                receive_data(socket);
                socket.close();
                receiverSocket.close();
                System.out.println("Socket closed - Transfer finished\n");
            } catch (IOException e) {
                LOGGER.error("Failed to connect data migration sender");
                return;
            }
        }

    }

    private void finish() {
        if (serverNode.getServerStatus().getFinalRange() == null){
            System.out.println("This ServerStatus getfinalRange() is null");
        } else {
            serverNode.setRange(serverNode.getServerStatus().getFinalRange());
        }
        serverNode.getServerStatus().setReady();
        System.out.println(serverNode.getNodeHostPort() +  "> servernode: " + gson.toJson(serverNode));
    }

    public void send_data(Socket senderSocket) throws IOException {
        ArrayList<String> keys = storageManager.returnKeysInRange(hashRange);
        for (String key : keys) {
            String value = storageManager.getKV(key);
            Message message = new Message(KVMessage.StatusType.PUT, 0, 0, key, value);
            transmission.sendMessage(toByteArray(gson.toJson(message)), senderSocket);

            Message recvMsg = transmission.receiveMessage(senderSocket);
            if (recvMsg.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
                storageManager.deleteRV(key);
                System.out.println(recvMsg.getStatus().toString());
            } else {
                //todo... possibly retry and send error message
                LOGGER.error("Sender: failed to migrate a packet");
            }
        }
        Message message = new Message(KVMessage.StatusType.CLOSE_REQ, 0, 0, null, null);
        transmission.sendMessage(toByteArray(gson.toJson(message)), senderSocket);
    }

    //need to send PUT_SUCCESS as ACK, then either return signal to main thread

    public void receive_data(Socket receiverSocket) throws IOException{
        Message data = transmission.receiveMessage(receiverSocket);
        System.out.println("Data received, " + data);
        while(data.getStatus() != KVMessage.StatusType.CLOSE_REQ) {
            if(data.getStatus() == KVMessage.StatusType.PUT) {
                System.out.println("Packets (" + data.getKey() +"," + data.getValue() + ") " + "received\n");
                storageManager.putKV(data.getKey(), data.getValue());

                Message retMessage = new Message(KVMessage.StatusType.PUT_SUCCESS, 0, 0, null, null);
                System.out.println("PUT_SUCCESS: sent ack to the sender");
                transmission.sendMessage(toByteArray(gson.toJson(retMessage)), receiverSocket);

                data = transmission.receiveMessage(receiverSocket);
            }
            else{
                LOGGER.error("Receiver: Wrong data migration status");
                break;
            }
        }
        System.out.println("Data received, " + data);
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


//    public static void main(String[] args){
//
//         {
//            try {
//                new LogSetup("logs/server.log", Level.DEBUG);
//            } catch (IOException e) {
//                System.out.println("Error! Unable to initialize logger!");
//                e.printStackTrace();
//                System.exit(1);
//            }
//        }
//
//        StorageManager sm = new StorageManager(1000, "LRU");
//        sm.clearAll();
//        BigInteger[] hashRange = new BigInteger[2];
//
//        String ip = "192.168.1.132";
//        String port = "50000";
//        String targetName = ip + ":"  + port;
//        ServerNode node = new ServerNode(targetName,  ip, Integer.parseInt(port));
//        ServerStatus initStatus = new ServerStatus(hashRange, targetName, ServerStatusType.MOVE_DATA_RECEIVER);
//        node.setServerStatus( initStatus);
//
//        KVServerDataMigration dut = new KVServerDataMigration(targetName, hashRange, node, sm);
//        dut.run();
//    }
}
