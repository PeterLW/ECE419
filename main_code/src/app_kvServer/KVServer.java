package app_kvServer;

import common.cache.StorageManager;
import common.zookeeper.ZNodeMessage;
import common.zookeeper.ZNodeMessageStatus;
import common.zookeeper.ZookeeperWatcher;
import ecs.ServerNode;
import logger.LogSetup;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.ServerSocket;

public class KVServer implements IKVServer {
    //log info
    private static final String PROMPT = "KVSERVER>";
    private static final Logger LOGGER = Logger.getLogger(KVServer.class);

    //connection info
    private int port;
    private String hostname = null;
    private ServerSocket serverSocket;
    private boolean running = false;
    private boolean stop = false;

	private static StorageManager storage;

	/* This needs to be passed into ClientConnections & ZookeeperWatcher thread */
	private static ServerNode serverNode;

	private static UpcomingStatusQueue upcomingStatusQueue = new UpcomingStatusQueue();

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

	// in progress
	public KVServer(String name, String zkHostname, int zkPort) { // m2 interface
		if (name == null){
			LOGGER.error("KVServer must have name argument to start up");
			System.exit(-1);
			return;
		}
		ZookeeperWatcher zookeeperWatcher = null;
		String zookeeperHost = zkHostname + ":" + Integer.toString(zkPort);
		try {
			zookeeperWatcher = new ZookeeperWatcher(zookeeperHost,100000,name, upcomingStatusQueue);
		} catch (IOException | InterruptedException e) {
			LOGGER.error("Failed to connect to zookeeper server");
			System.exit(-1);
		}

		try { // get serverNode
			ZNodeMessage znodeMessage = zookeeperWatcher.getZnodeMessage();
			serverNode = znodeMessage.serverNode;
			zookeeperWatcher.setServerNode(serverNode); // zookeeperWatcher may change this when receive data updates

			ServerStatus ss;
			if (znodeMessage.zNodeMessageStatus == ZNodeMessageStatus.NEW_ZNODE_RECEIVE_DATA) {
				ss = new ServerStatus(ServerStatusType.MOVE_DATA_RECEIVER); // move data auto transits to Running when isReady = true
			} else {
				ss = new ServerStatus(ServerStatusType.INITIALIZE);
			}
			serverNode.setServerStatus(ss);
		} catch (KeeperException | InterruptedException e){
			LOGGER.error("Failed to get data from zNode ",e);
			System.exit(-1);
		}
		storage = new StorageManager(serverNode.getCacheSize(), serverNode.getCacheStrategy());
		zookeeperWatcher.run(); // NOW IT SETS THE WATCH AND WAITS FOR DATA CHANGES

		KVClientConnection kvClientConnection = new KVClientConnection(storage,serverNode,zookeeperHost,10000);
		kvClientConnection.run();

        KVServerDataMigration dataMigration = new KVServerDataMigration(serverNode, storage);
        dataMigration.run();
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
//		LOGGER.info(">Server port: " + this.port);
		return port;
	}

	@Override
    public String getHostname(){
//		LOGGER.info("Server hostname: " + hostname);
		return hostname;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
        //LOGGER.info("Server ("+hostname+","+port+") : CacheManager Strategy is "+ cacheStrategy);
		return string_to_enum_cache_strategy(serverNode.getCacheStrategy());
	}

	@Override
    public int getCacheSize(){
		return serverNode.getCacheSize();
	}

    @Override
    public boolean inStorage(String key){
        if(key != null && !(key.isEmpty()) && !(key.equals("")) && !(key.contains(" ")) && !(key.length() > 20)) {
            return storage.inDatabase(key);
        }
        else{
            return false;
        }
	}

	@Override
    public boolean inCache(String key){
		return storage.inCache(key);
	}

	@Override
    public String getKV(String key) throws Exception{
        return storage.getKV(key);
	}

	@Override
    public void putKV(String key, String value) throws Exception{
        if(storage.putKV(key, value)){
            LOGGER.info("Server ("+hostname+","+port+") : Success in putKV");
        }
        else{
            LOGGER.info("Server ("+hostname+","+port+") : Error in putKV");
        }
	}

	@Override
    public void clearCache(){
		storage.clearCache();
		return;
	}

	@Override
    public void clearStorage(){
		storage.clearAll();
	}

	public void run(){ // status
        while (true) {
            if (upcomingStatusQueue.peakQueue() != null) {
                ServerStatus next = upcomingStatusQueue.peakQueue();
                ServerStatus curr = serverNode.getServerStatus();

                boolean proceed = true;
                switch (curr.getStatus()) {
                    case INITIALIZE:
                        if (next.getTransition() == ZNodeMessageStatus.START_SERVER) {
                            next.setServerStatus(ServerStatusType.RUNNING);
                            serverNode.setServerStatus(next);
                        }
                        break;
                    case RUNNING:
                        if (next.getTransition() == ZNodeMessageStatus.STOP_SERVER) {
                            next.setServerStatus(ServerStatusType.IDLE);
                            serverNode.setServerStatus(next);
                        } else if (next.getTransition() == ZNodeMessageStatus.MOVE_DATA_RECEIVER) {
                            next.setServerStatus(ServerStatusType.MOVE_DATA_RECEIVER);
                            serverNode.setServerStatus(next);
                        } else if (next.getTransition() == ZNodeMessageStatus.MOVE_DATA_SENDER) {
							next.setServerStatus(ServerStatusType.MOVE_DATA_SENDER);
							serverNode.setServerStatus(next);
						}
                        break;
                    case IDLE:
                        if (next.getTransition() == ZNodeMessageStatus.START_SERVER) {
                            next.setServerStatus(ServerStatusType.RUNNING);
                            serverNode.setServerStatus(next);
                        }
                        break;
                    case MOVE_DATA_RECEIVER:
                    case MOVE_DATA_SENDER:
                        if (curr.isReady()) {
                            next.setServerStatus(ServerStatusType.RUNNING);
                            serverNode.setServerStatus(next);
                            proceed = false;
                        }
                        break;
                    case CLOSE:
                        proceed = false;
                }

                if (proceed) {
                    upcomingStatusQueue.popQueue();
                }
            }
        }
	}

	@Override
    public void kill(){ //here kill( ) will be same as close( ) as we are using write-through cache. For now, leave it as the same as close()
		// TODO
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.error("Error! " + "Unable to close socket on port: " + port, e);
        }
        running = false;
		stop = true;
	}

	@Override
    public void close(){
		// TODO
		try {
			serverSocket.close();
		} catch (IOException e) {
			LOGGER.error("Error! " + "Unable to close socket on port: " + port, e);
		}
		running = false;
		stop = true;
	}

	//TODO
	@Override
	public void start() {

	}

	@Override
	public void stop() {
	}


	@Override
	public void lockWrite() {

	}

	@Override
	public void unlockWrite() {

	}

	@Override
	public boolean moveData(String[] hashRange, String targetName) throws Exception {
		return false;
	}


	/**
	 * java m2-server.jar -name ServerID -zkhost HOST -zkport PORT
	 *
	 * ServerID: name used in zookeeper
	 * getHostIpPort
	 *
	 */
	public static void main(String[] args){
		//TODO read from cmdline the arguments needed to start KVServer
		Options options = new Options();

		options.addOption("name",true,"The serverID, getHostIpPort()");
		options.getOption("name").setRequired(true);
		options.addOption("zkhost",true,"Zookeeper Host (IP Address)");
		options.addOption("zkport",true,"Zookeeper port #");
		options.getOption("zkport").setType(Integer.class);

		CommandLineParser cmdLineParser = new DefaultParser();

		// default values
		String name = null, zkhost = "localhost";
		int zkport = 2185; // double check

		try {
			CommandLine cmdLine = cmdLineParser.parse(options, args);

			if (cmdLine.hasOption("name")){
				name = cmdLine.getOptionValue("name").toString();
			}

			if (cmdLine.hasOption("zkhost")){
				zkhost = cmdLine.getOptionValue("zkhost").toString();
			}

			if (cmdLine.hasOption("zkport")){
				zkport = Integer.parseInt(cmdLine.getOptionValue("zkport"));
			}
		} catch (ParseException e) {
			LOGGER.error("Error parsing command line arguments", e);
			System.exit(-1);
		}
		KVServer kvServer = new KVServer(name,zkhost,zkport);
		kvServer.run();
	}
}
