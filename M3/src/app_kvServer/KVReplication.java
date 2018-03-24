package app_kvServer;

import app_kvServer.ClientConnection;
import app_kvServer.IKVServer;
import app_kvServer.ServerStatus;
import common.cache.StorageManager;
import common.metadata.Metadata;
import common.zookeeper.ZookeeperWatcher;
import ecs.ServerNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.google.gson.Gson;
import java.math.*;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import common.messages.KVMessage;
import common.messages.Message;
import common.transmission.Transmission;


public class KVReplication extends Thread {
    //log info
    private static final Logger LOGGER = Logger.getLogger(app_kvServer.KVReplication.class);
    //connection info
    private ServerSocket serverSocket;
    StorageManager storageManager;
    private static ServerNode serverNode;
    private int sessionTimeout;
    private Transmission transmission;
    private int port;
    private final Gson gson = new Gson();
    private final static int REPLICATION_PORT_OFFSET = 51;


    static {
        try {
            new LogSetup("logs/KVReplication.log", Level.ERROR);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    //todo: inccomplete now
    public KVReplication(ServerNode node, StorageManager storageManager,int sessionTimeout){
    
        this.serverNode = node;
        this.sessionTimeout = sessionTimeout;
        this.storageManager = storageManager;
        this.transmission = new Transmission();
    }

    @Override
    public void run() {
        if(!initializeServer()){
            return;
        }
        while (true) {

                    Socket client = null;
                    try {
                        client = serverSocket.accept();
                        String clientIdString = Integer.toString(0);
                        transmission.sendMessage(toByteArray(clientIdString), client);
                        receive_data(client);
                        client.close();
                        System.out.println("localhost:" + Integer.toString(port) + " replication connection closed\n");

                    } catch (SocketTimeoutException e) {
                        if (serverNode.getServerStatus().getStatus() == ServerStatusType.CLOSE) {
                            close();
                        }
                    } catch (IOException e) {
                        LOGGER.error("Error! " + "Unable to establish connection. \n");
                    }
                    if (serverNode.getServerStatus().getStatus() == ServerStatusType.CLOSE) {
                        try {
                            serverSocket.close();
                            // System.exit(0);
                            break;
                        } catch (IOException e) {
                            LOGGER.error("Error! " + "Unable to close connection. \n");
                        }
                    }
        }
    }

    private void receive_data(Socket receiverSocket) throws IOException{

        Message data = transmission.receiveMessage(receiverSocket);
        while(!(data.getStatus() == KVMessage.StatusType.CLOSE_REQ)) {
            if(data.getStatus() == KVMessage.StatusType.PUT) {

                System.out.println("localhost:" +Integer.toString(port) + ": Packets (" + data.getKey() +"," + data.getValue() + ") " + "received\n");

                //replicaDataManager.addReplicaData(data.getClientID(),data.getKey(), data.getValue());
                storageManager.putKV(data.getKey(), data.getValue());

                Message retMessage = new Message(KVMessage.StatusType.PUT_SUCCESS, 0, 0, null, null);
                System.out.println("localhost:" +Integer.toString(port) + ": PUT_SUCCESS: sent ack to the sender");
                transmission.sendMessage(toByteArray(gson.toJson(retMessage)), receiverSocket);
                data = transmission.receiveMessage(receiverSocket);
            }
        }
        System.out.println("localhost:" +Integer.toString(port) + " finished receiving packets\n");
    }

    private boolean initializeServer() {

        try {
            port = this.serverNode.getNodePort() + REPLICATION_PORT_OFFSET;
            this.serverSocket = new ServerSocket(port);
            System.out.println("KVReplication starts on port: " + Integer.toString(port));
            this.serverSocket.setSoTimeout(sessionTimeout); // 1 s
            return true;
        } catch (IOException e) {
            LOGGER.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
                LOGGER.error("Port " + this.serverNode.getNodePort() + " is already bound!");
            }
            return false;
        }
    }

    private void close(){
        try {
            serverSocket.close();
            System.out.println("localhost:" + Integer.toString(port) + " is closed now\n");
           
        } catch (IOException e) {
            LOGGER.error("Error! " + "Unable to close socket on port: " + serverNode.getNodePort(), e);
        }
    }

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

}
