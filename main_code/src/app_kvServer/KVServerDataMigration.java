package app_kvServer;

import common.cache.StorageManager;
import common.messages.KVMessage;
import common.messages.Message;
import common.transmission.Transmission;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServerDataMigration implements  Runnable {

    private  String address;
    private int port;
    private BigInteger[] hashRange;
    private ServerStatusType status;
    private Transmission transmission;
    StorageManager storageManager;
    private final static Logger LOGGER = Logger.getLogger(KVServerDataMigration.class);

    public KVServerDataMigration(String targetName, BigInteger[] hashRange, ServerStatusType status, StorageManager storageManager){
        String[] temp = targetName.split(":");
        this.address = temp[0];
        this.port = Integer.parseInt(temp[1]);

        this.hashRange = hashRange;
        this.status = status;
        this.transmission = new Transmission();
        this.storageManager = storageManager;
    }

    public void run() {
        if(status == ServerStatusType.MOVE_DATA_SENDER){
            try {
                Socket senderSocket = new Socket(address, port);
                send_data(senderSocket);
            } catch (IOException e) {
                LOGGER.error("Failed to connect data migration receiver");
            }
        }
        else if(status == ServerStatusType.MOVE_DATA_RECEIVER){
            try {
                ServerSocket receiverSocket = new ServerSocket(port);
                Socket socket = receiverSocket.accept();

                Message data = transmission.receiveMessage(socket);
                while(!(data.getStatus() == KVMessage.StatusType.CLOSE_REQ)) {
                    if(data.getStatus() == KVMessage.StatusType.PUT) {
                        storageManager.putKV(data.getKey(), data.getValue());
                        data = transmission.receiveMessage(socket);
                    }
                    else{
                        LOGGER.error("Wrong data migration status");
                        break;
                    }
                }

                socket.close();
                receiverSocket.close();
            } catch (IOException e) {
                LOGGER.error("Failed to connect data migration sender");
            }
        }
        else{

        }
    }

    public void send_data(Socket senderSocket){

    }

}
