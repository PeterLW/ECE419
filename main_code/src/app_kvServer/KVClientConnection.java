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

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

// IN PROGRESS
public class KVClientConnection implements Runnable {
    //log info
    private static final Logger LOGGER = Logger.getLogger(app_kvServer.KVClientConnection.class);
    //connection info
    private ServerSocket serverSocket;
    private boolean stop = false;
    private static int numConnectedClients = 0;
    //cache info - stored in ServerNode now
    private static StorageManager storage;
    /* This needs to be passed into ClientConnections & ZookeeperWatcher thread */
    private static ServerNode serverNode;
    private String zookeeperHost;
    private int sessionTimeout;

    static {
        try {
            new LogSetup("logs/kvclientConncection.log", Level.DEBUG);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    //todo: inccomplete now
    public KVClientConnection(StorageManager storageManager, ServerNode node,String zookeeperHost, int sessionTimeout ){

        this.storage = storageManager;
        this.serverNode = node;
        this.zookeeperHost = zookeeperHost;
        this.sessionTimeout = sessionTimeout;
    }

    public void run() {

        initializeServer();
        while (!this.stop) {
            // waits for connection
            if (this.serverSocket != null) {
                Socket client = null;
                try {
                    client = serverSocket.accept(); // blocking call
                    numConnectedClients++;
                    ClientConnection connection = new ClientConnection(client, serverNode, storage, numConnectedClients, zookeeperHost, sessionTimeout);
                    LOGGER.info("Connected to " + client.getInetAddress().getHostName() + " on port " + client.getPort());
                    new Thread(connection).start();

                } catch (SocketTimeoutException e) {
						/* don't really need to do anything, the timeout is so that periodically,
						 KVServer will check to see if the ServerStatus changed
						*/
                } catch (IOException e) {
                    LOGGER.error("Error! " + "Unable to establish connection. \n");
                }
            }
        }
    }

    private boolean initializeServer() {
        LOGGER.info("Initialize server ...");
        try {
            this.serverSocket = new ServerSocket(this.serverNode.getNodePort());
            LOGGER.info("Server listening on port: " + this.serverNode.getNodePort());
            this.serverSocket.setSoTimeout(1000); // 1 s
            return true;
        } catch (IOException e) {
            LOGGER.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
                LOGGER.error("Port " + this.serverNode.getNodePort() + " is already bound!");
            }
            return false;
        }
    }

    public void kill(){ //here kill( ) will be same as close( ) as we are using write-through cache. For now, leave it as the same as close()
        // TODO Auto-generated method stub
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.error("Error! " + "Unable to close socket on port: " + serverNode.getNodePort(), e);
        }
        stop = true;
    }

    public void close(){
        // TODO Auto-generated method stub
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.error("Error! " + "Unable to close socket on port: " + serverNode.getNodePort(), e);
        }
        stop = true;
    }

    public static void main(String[] args){
        //TODO read from cmdline the arguments needed to start KVServer
    }
}
