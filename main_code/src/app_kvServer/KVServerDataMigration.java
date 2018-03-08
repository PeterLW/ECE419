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

    public KVServerDataMigration(String targetName, BigInteger[] hashRange, ServerNode serverNode, StorageManager storageManager){
        String[] temp = targetName.split(":");
        this.address = temp[0];
        this.port = Integer.parseInt(temp[1]);

        this.hashRange = hashRange;
        this.serverNode = serverNode;
        this.transmission = new Transmission();
        this.storageManager = storageManager;
    }

    //Q: where is the kill handler ? (i.e. what if the main thread sends a kill signal here , does
    //cathy use this class as an object or run this thread at startup ? Need a mechanism to kill and close this thread.
    public void run() {

        System.out.println("system starts ....\n");

        while(true) {
            if (serverNode.getServerStatus().getStatus() == ServerStatusType.MOVE_DATA_SENDER) {
                try {
                    Socket senderSocket = new Socket(address, port);
                    System.out.println("connecting to receiver...");
                    send_data(senderSocket);
                    senderSocket.close();
                } catch (IOException e) {
                    LOGGER.error("Failed to connect data migration receiver");
                }
                break;
            } else if (serverNode.getServerStatus().getStatus() == ServerStatusType.MOVE_DATA_RECEIVER) {
                try {
                    ServerSocket receiverSocket = new ServerSocket(port);
                    Socket socket = receiverSocket.accept();
                    System.out.println("Connected to the sender, start receiving packets\n");
                    receive_data(socket);
                    socket.close();
                    receiverSocket.close();
                    System.out.println("Socket closed\n");
                } catch (IOException e) {
                    LOGGER.error("Failed to connect data migration sender");
                }
            } else {
                try {
                    Thread.sleep(1);

                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }
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
                LOGGER.error("Receiver: Wrong data migration status");
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
