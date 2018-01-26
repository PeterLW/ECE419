package app_kvServer;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

import common.cache.CacheManager;
import common.cache.CacheStructure;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import common.disk.DBManager;

public class KVServer implements IKVServer {

    //log info
    private static final String PROMPT = "KVSERVER>";
    private static final Logger LOGGER = Logger.getLogger(KVServer.class);
    private static DBManager dbManager;

    //connection info
    private int port;
    private String hostname = null;
    private ServerSocket serverSocket;
    private boolean running = false;
    private boolean stop = false;

    private static int numConnectedClients = 0;

    //cache info
	private int cacheSize;
	private String cacheStrategy;
	private static CacheManager caching;

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

	static {
		try {
			new LogSetup("logs/server.log", Level.DEBUG);
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.port = port;
		this.cacheSize = cacheSize;
		this.cacheStrategy = strategy;
        dbManager = new DBManager();
		caching = new CacheManager(cacheSize, strategy,dbManager);

	}

    public boolean isRunning() {
        return this.running;
    }

	private CacheStrategy string_to_enum_cache_strategy(String str) {
		switch (str.toLowerCase()){
			case "LRU":
				return CacheStrategy.LRU;
			case "LFU":
				return CacheStrategy.LFU;
			case "FIFO":
				return CacheStrategy.FIFO;
			default:
				return CacheStrategy.None;
		}
	}

	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		LOGGER.info("Server port: " + port);
		return port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stubc
		LOGGER.info("Server hostname: " + hostname);
		return hostname;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
        LOGGER.info("Server ("+hostname+","+port+") : CacheManager Strategy is "+ cacheStrategy);
		return string_to_enum_cache_strategy(cacheStrategy);
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return cacheSize;
	}

    @Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
        if(key == null || key.isEmpty() == true) {
            return dbManager.isExists(key);
        }
        else{
            return false;
        }
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return caching.inCache(key);
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
            LOGGER.info("Server ("+hostname+","+port+") : Success in putKV");
        }
        else{
            LOGGER.info("Server ("+hostname+","+port+") : Error in putKV");
        }
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
        dbManager.clearStorage();
	}

	public void run(){
		// TODO Auto-generated method stub
		this.running = initializeServer();
		while(!this.stop) {
			// waits for connection
			if(this.serverSocket != null) {
				while(isRunning()){
					try {
						Socket client = serverSocket.accept(); // blocking call
						numConnectedClients++;
						ClientConnection connection = new ClientConnection(client, caching, numConnectedClients);
						LOGGER.info("Connected to " + client.getInetAddress().getHostName()
								+  " on port " + client.getPort());
						new Thread(connection).start();
					} catch (IOException e) {
						LOGGER.error("Error! " +  "Unable to establish connection. \n", e);
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
			LOGGER.info("Server listening on port: " + this.serverSocket.getLocalPort());
			return true;
		} catch (IOException e) {
			LOGGER.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				LOGGER.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	@Override
    public void kill(){ //here kill( ) will be same as close( ) as we are using write-through cache. For now, leave it as the same as close()
		// TODO Auto-generated method stub
        running = false;
        try {
            serverSocket.close();

        } catch (IOException e) {
            LOGGER.error("Error! " + "Unable to close socket on port: " + port, e);
        }
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			LOGGER.error("Error! " + "Unable to close socket on port: " + port, e);
		}
	}

	public static void main(String[] args){
			KVServer server = new KVServer(50000,10,"LRU"); // these should be from cmdline
			server.run();
	}
}
