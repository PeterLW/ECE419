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
import java.math.*;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;

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
    private Metadata metadata;

    static {
        try {
            new LogSetup("logs/kvclientConncection.log", Level.ERROR);
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
        this.metadata = new Metadata();
    }
/* The following setter and getter are used for debugging purpose */

    public Metadata getMetadata(){
        return metadata;
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
                   // LOGGER.info("Connected to " + client.getInetAddress().getHostName() + " on port " + client.getPort());
                    System.out.println("Connected to " + client.getInetAddress().getHostName() + " on port " + client.getPort());
                    new Thread(connection).start();

                } catch (SocketTimeoutException e) {
						/* don't really need to do anything, the timeout is so that periodically,
						 KVServer will check to see if the ServerStatus changed
						*/
                } catch (IOException e) {
                    LOGGER.error("Error! " + "Unable to establish connection. \n");
                }
                if(serverNode.getServerStatus().getStatus() == ServerStatusType.CLOSE){
                    try {
                        serverSocket.close();
                       // System.exit(0);
                        break;
                    }catch (IOException e){
                        LOGGER.error("Error! " + "Unable to close connection. \n");
                    }
                }
            }
        }
    }

    private boolean initializeServer() {
//        LOGGER.info("Initialize server ...");
        System.out.println("Initialize server ...");
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
        StorageManager storageManager = new StorageManager(1000, "FIFO");
        String zookeeperHost = "localhost";
        int sessionTimeout=5000;

        ServerNode node = new ServerNode("test_server", "0.0.0.0", 50000);
        node.setServerStatus(new ServerStatus(ServerStatusType.RUNNING));
        BigInteger[] range = new BigInteger[2];
        try {
            range[0] = getMD5("a");
            range[1] = getMD5("d");
        } catch (Exception e) {
            e.printStackTrace();
        }
        node.setRange(range);

        KVClientConnection kc = new KVClientConnection(storageManager,  node, zookeeperHost,  sessionTimeout);

       // kc.getMetadata().addServer(hostname);
        kc.run();
    }

    private static BigInteger getMD5(String input) throws Exception{
        MessageDigest md=MessageDigest.getInstance("MD5");
        md.update(input.getBytes(),0,input.length());
        String hash_temp = new BigInteger(1,md.digest()).toString(16);
        BigInteger hash = new BigInteger(hash_temp, 16);
        return hash;
    }
}
