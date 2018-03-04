package app_kvServer;
import app_kvServer.ClientConnection;
import app_kvServer.IKVServer;
import app_kvServer.ServerStatus;
import com.sun.security.ntlm.Server;
import common.cache.StorageManager;
import common.Metadata.Metadata;
import common.zookeeper.ZookeeperWatcher;
import ecs.ServerNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

// IN PROGRESS
public class KVClientConnection implements Runnable {
    //log info

    public enum CacheStrategy_t {
        None,
        LRU,
        LFU,
        FIFO
    };

    private static final Logger LOGGER = Logger.getLogger(app_kvServer.KVClientConnection.class);

    //connection info
    private int port;
    private String hostname = null;
    private ServerSocket serverSocket;
    private boolean stop = false;
    private boolean running = false;
    private static int numConnectedClients = 0;

    //cache info - stored in ServerNode now
    private static StorageManager storage;

    private static Metadata metadata; //need it to transfer to KVClient
    /* This needs to be passed into ClientConnections & ZookeeperWatcher thread */
    private static ServerNode serverNode;


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
    public KVClientConnection(StorageManager storageManager){


    }



    private CacheStrategy_t string_to_enum_cache_strategy(String str) {
        switch (str.toLowerCase()){
            case "LRU":
                return CacheStrategy_t.LRU;
            case "LFU":
                return CacheStrategy_t.LFU;
            case "FIFO":
                return CacheStrategy_t.FIFO;
            default:
                return CacheStrategy_t.None;
        }
    }


    public int getPort(){
        // TODO Auto-generated method stub
//		LOGGER.info(">Server port: " + this.port);
        return port;
    }

    public String getHostname(){
        // TODO Auto-generated method stubc
//		LOGGER.info("Server hostname: " + hostname);
        return hostname;
    }

    public CacheStrategy_t getCacheStrategy(){
        // TODO Auto-generated method stub
        //LOGGER.info("Server ("+hostname+","+port+") : CacheManager Strategy is "+ cacheStrategy);
        return string_to_enum_cache_strategy(serverNode.getCacheStrategy());
    }

    public int getCacheSize(){
        // TODO Auto-generated method stub
        return serverNode.getCacheSize();
    }

    public boolean inStorage(String key){
        // TODO Auto-generated method stub
        if(key != null && !(key.isEmpty()) && !(key.equals("")) && !(key.contains(" ")) && !(key.length() > 20)) {
            return storage.inDatabase(key);
        }
        else{
            return false;
        }
    }

    public boolean inCache(String key){
        // TODO Auto-generated method stub
        return storage.inCache(key);
    }

    public String getKV(String key) throws Exception{
        // TODO Auto-generated method stub
        return storage.getKV(key);
    }

    public void putKV(String key, String value) throws Exception{
        // TODO Auto-generated method stub
        if(storage.putKV(key, value)){
            LOGGER.info("Server ("+hostname+","+port+") : Success in putKV");
        }
        else{
            LOGGER.info("Server ("+hostname+","+port+") : Error in putKV");
        }
    }

    public void run(){
        // TODO Auto-generated method stub
        this.running = initializeServer();
        while(!this.stop) {
            // waits for connection
            if(this.serverSocket != null) {

                if (serverNode.getServerStatus() == ServerStatus.STARTING){
                    // Starting:
                    // get Metadata object,
                }

                while(serverNode.getServerStatus() == ServerStatus.RUNNING){
                    Socket client = null;
                    try {
                        client = serverSocket.accept(); // blocking call
                        numConnectedClients++;
                        ClientConnection connection = new ClientConnection(client, storage, numConnectedClients);
                        LOGGER.info("Connected to " + client.getInetAddress().getHostName() + " on port " + client.getPort());
                        new Thread(connection).start();
                    } catch (SocketTimeoutException e){
						/* don't really need to do anything, the timeout is so that periodically,
						 KVServer will check to see if the ServerStatus changed
						*/
                    } catch (IOException e) {
                        LOGGER.error("Error! " +  "Unable to establish connection. \n");
                    }
                }

            }
        }
    }

    private boolean initializeServer() {
        LOGGER.info("Initialize server ...");
        try {
            this.serverSocket = new ServerSocket(this.port);
            this.hostname = serverSocket.getInetAddress().getHostName();
            this.port = this.serverSocket.getLocalPort();
            LOGGER.info("Server listening on port: " + this.serverSocket.getLocalPort());
            this.serverSocket.setSoTimeout(1000); // 1 s
            return true;
        } catch (IOException e) {
            LOGGER.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
                LOGGER.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    public void kill(){ //here kill( ) will be same as close( ) as we are using write-through cache. For now, leave it as the same as close()
        // TODO Auto-generated method stub
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.error("Error! " + "Unable to close socket on port: " + port, e);
        }
        running = false;
        stop = true;
    }

    public void close(){
        // TODO Auto-generated method stub
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.error("Error! " + "Unable to close socket on port: " + port, e);
        }
        running = false;
        stop = true;
    }

    /*

    They will be invoked by having listening the current server status.

     */

    public void start() {
    }

    public void stop() {
        serverNode.setServerStatus(ServerStatus.STOPPED);
    }

    public void lockWrite() {

    }

    public void unlockWrite() {

    }
    
    public boolean moveData(String[] hashRange, String targetName) throws Exception {
        return false;
    }

    public static void main(String[] args){
        //TODO read from cmdline the arguments needed to start KVServer
//			KVServer server = new KVServer(50000,10,"LRU"); // these should be from cmdline
//			server.run();
    }
}
