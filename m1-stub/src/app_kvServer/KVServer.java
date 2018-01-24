package app_kvServer;
import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import app_kvClient.KVClient;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import app_kvServer.ClientConnection;
import common.cache.Cache;
import common.disk.DBManager;

public class KVServer implements IKVServer {

    //log info
    private static final Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVSERVER>";
    private static DBManager db_manger;

    //connection info
    private int port;
    private String hostname = null;
    private ServerSocket serverSocket;
    private boolean running = false;
    private boolean stop = false;

    //cache info
	private  int cache_size;
	private String cache_strategy;
	private static Cache caching;

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.port = port;
		this.cache_size = cacheSize;
		this.cache_strategy = strategy;
        this.db_manger = new DBManager();
		this.caching = new Cache(cacheSize, strategy,this.db_manger);

	}

	public void connect(){

		running = initializeServer();

		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client, caching);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

    public boolean isRunning() {
        return this.running;
    }

	private CacheStrategy string_to_enum_cache_strategy(String str) {
		if(str.equals("LRU"))
			return CacheStrategy.LRU;
		else if(str.equals("LFU"))
			return CacheStrategy.LFU;
		else if(str.equals("FIFO"))
			return CacheStrategy.FIFO;
		else
			return CacheStrategy.None;
	}

	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		logger.info("Server port: " + port);
		return port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stubc
		logger.info("Server hostname: " + hostname);
		return hostname;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
        logger.info("Server ("+hostname+","+port+") : Cache Strategy is "+ cache_strategy);
		return string_to_enum_cache_strategy(cache_strategy);
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return cache_size;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return db_manger.isExists(key);
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return caching.in_cahce(key);
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
        return caching.getKV(key);
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
        if(caching.putKV(key, value) == true){
            logger.info("Server ("+hostname+","+port+") : Success in putKV");
        }
        else{
            logger.info("Server ("+hostname+","+port+") : Error in putKV");
        }
        return;
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
		caching.clear();
		return;
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
        db_manger.clearStorage();
        return;
	}

	@Override
	public void run(){
		// TODO Auto-generated method stub
		this.running = initializeServer();
		while(!this.stop) {
			// waits for connection
			if(this.serverSocket != null) {
				while(isRunning()){
					try {
						Socket client = serverSocket.accept(); // blocking call
						ClientConnection connection = new ClientConnection(client, caching);
						logger.info("Connected to " + client.getInetAddress().getHostName()
								+  " on port " + client.getPort());
						new Thread(connection).start();
					} catch (IOException e) {
						logger.error("Error! " +  "Unable to establish connection. \n", e);
					}
				}
			}
		}
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			this.serverSocket = new ServerSocket(this.port);
            this.hostname = serverSocket.getInetAddress().getHostName();
			logger.info("Server listening on port: " + this.serverSocket.getLocalPort());
			return true;
		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	@Override
    public void kill(){ //here kill( ) will be same as close( ) as we are using write-through cache
		// TODO Auto-generated method stub
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    return;
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
		return;
	}

	public static void main(String[] args){
		try {
			new LogSetup("logs/client.log", Level.INFO); // debug - setting log to info level
			KVServer server = new KVServer(2000,200,"LRU"); // these should be from cmdline
			server.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}


}
