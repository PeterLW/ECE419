package app_kvServer;

import common.cache.StorageManager;
import common.messages.KVMessage;
import common.messages.Message;
import common.transmission.Transmission;
import org.apache.log4j.Logger;
import com.google.gson.Gson;

import java.io.IOException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import ecs.ServerNode;

public class KVServerDataMigration implements  Runnable {

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
        while(serverNode.getServerStatus().getStatus() != ServerStatusType.MOVE_DATA_SENDER &&
                serverNode.getServerStatus().getStatus() != ServerStatusType.MOVE_DATA_RECEIVER) {

            if (serverNode.getServerStatus().getStatus() == ServerStatusType.MOVE_DATA_SENDER) {
                try {
                    Socket senderSocket = new Socket(address, port);
                    send_data(senderSocket);
                    senderSocket.close();
                } catch (IOException e) {
                    LOGGER.error("Failed to connect data migration receiver");
                }
            } else if (serverNode.getServerStatus().getStatus() == ServerStatusType.MOVE_DATA_RECEIVER) {
                try {
                    ServerSocket receiverSocket = new ServerSocket(port);
                    Socket socket = receiverSocket.accept();
                    receive_data(socket);
                    socket.close();
                    receiverSocket.close();
                } catch (IOException e) {
                    LOGGER.error("Failed to connect data migration sender");
                }
            } else {

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
                storageManager.putKV(data.getKey(), data.getValue());

                Message retMessage = new Message(KVMessage.StatusType.PUT_SUCCESS, 0, 0, null, null);
                transmission.sendMessage(toByteArray(gson.toJson(retMessage)), receiverSocket);

                data = transmission.receiveMessage(receiverSocket);
            }
            else{
                LOGGER.error("Receiver: Wrong data migration status");
                break;
            }
        }
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

    public void main(){

    }

}
