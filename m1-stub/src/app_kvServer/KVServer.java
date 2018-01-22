package app_kvServer;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import app_kvServer.ClientConnection;
import common.cache.Cache;

public class KVServer implements IKVServer {

    //log info
    private static Logger logger = Logger.getRootLogger();

    //connection info
    private int port;
    private ServerSocket serverSocket;
    private boolean running;

    //cache info
	private  int cache_size;
	private String cache_strategy;
	Cache caching;

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
		caching = new Cache(cacheSize, strategy);
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

	public boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
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
	public int getPort(){
		// TODO Auto-generated method stub
		logger.info("Server port: " + port);
		return port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		logger.info("Server hostname: " + serverSocket.getInetAddress().getHostName());
		return serverSocket.getInetAddress().getHostName();
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		logger.info("Server Cache Strategy: " + cache_strategy);
		return string_to_enum_cache_strategy(cache_strategy);
	}

	public CacheStrategy string_to_enum_cache_strategy(String str) {
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
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return cache_size;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return caching.in_cahce(key);
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		if(caching.in_cahce(key))
			return caching.getKV(key);
		else {
			System.out.println("NOT IN CACHE");
			return "NOT IN CACHE";
		}
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		caching.putKV(key, value);
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
		caching.clear();
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub

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
	}

	public static void main(String[] args){
		try {
			new LogSetup("logs/server.log", Level.ALL);
			KVServer kvserver = new KVServer(1200, 4000, "LFU");
			kvserver.connect();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}


}
