package app_kvServer;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;

import client.KVStore;
import com.google.gson.Gson;
import common.metadata.Metadata;
import common.cache.StorageManager;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.Message;
import ecs.ServerNode;
import org.apache.log4j.*;
import common.transmission.Transmission;
import java.lang.String;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.util.HashSet;
import common.zookeeper.ZookeeperMetaData;
import org.apache.zookeeper.KeeperException;
import logger.LogSetup;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection extends Thread {
	private static final Logger LOGGER = Logger.getLogger(ClientConnection.class);
	private final Gson gson = new Gson();
	private Transmission transmission = new Transmission();
	private boolean isOpen = true;
	private StorageManager storageManager;
	private Socket clientSocket;
	private int clientId;
	private HashSet<Integer> seqIdValues = new HashSet<Integer>();
	private ZookeeperMetaData zookeeperMetaData;
	private ServerNode serverNode;
	private KVStore kvStore;
	private final static int REPLICATION_PORT_OFFSET = 51;

	   static {
        try {
            new LogSetup("logs/clientconnection.log", Level.ERROR);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

	/**
	 * Constructs a new ClientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, ServerNode node ,StorageManager caching, int clientId, String zookeeperHost, int sessionTimeout) {
		this.clientSocket = clientSocket;
		this.clientId = clientId;
		// initialized when declared above
		this.isOpen = true;
		this.transmission = new Transmission();
		this.storageManager = caching;

		try {
			this.zookeeperMetaData = new ZookeeperMetaData(zookeeperHost, sessionTimeout);
		}catch(IOException | InterruptedException e){
			e.printStackTrace();
		}

		this.serverNode = node;
		String clientIdString = Integer.toString(clientId);
		transmission.sendMessage(toByteArray(clientIdString),clientSocket);
	}

    private BigInteger getMD5(String input) throws Exception{
        MessageDigest md=MessageDigest.getInstance("MD5");
        md.update(input.getBytes(),0,input.length());
        String hash_temp = new BigInteger(1,md.digest()).toString(16);
        BigInteger hash = new BigInteger(hash_temp, 16);
        return hash;
    }

    private boolean isKeyInValidRange(String key){
        BigInteger[] range = serverNode.getRange();
        BigInteger hashValue = null;
        try {
             hashValue = getMD5(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(range[0].compareTo(range[1]) == 0) { //if there is only one server running
            return true;
        }
        else if(range[0].compareTo(range[1]) > 0){ //if key is in the first node, range[0]>range[1] in the hash ring
            if((hashValue.compareTo(range[0]) > 0 ) || (hashValue.compareTo(range[1]) < 0 )){
                return true;
            }
            else
                return false;
        }
        else{ //normal case
            if((hashValue.compareTo(range[0]) > 0) && (hashValue.compareTo(range[1]) < 0)){
                return true;
            }
            else
                return false;
        }
    }

    private void RespondMsg(Message received_msg, KVMessage.StatusType responseType, Metadata metaData){

	    if(received_msg == null){
	        return;
        }
        if(received_msg.getStatus() == StatusType.CLOSE_REQ){
            // System.out.println(serverNode.getNodeHostPort() + " starts to process CLOSE message\n");
            processMessage(received_msg);
        }
	    Message return_msg = new Message(responseType, clientId, received_msg.getSeq(), received_msg.getKey(), received_msg.getValue());
        String metadata = gson.toJson(metaData);
        return_msg.setMetaData(metadata);
        System.out.println("RespondMsg: reached here\n");
        System.out.println(gson.toJson(return_msg));
        boolean success = transmission.sendMessage(toByteArray(gson.toJson(return_msg)), clientSocket);
	    if (!success) {
	        LOGGER.error("Send message failed to client " + this.clientId);
            System.out.println("Send message failed to client " + this.clientId);
	    }
    }

	private boolean isHashInReplicaRange(String key, BigInteger hash, BigInteger[] replicaRange){

		if(replicaRange[1].compareTo(replicaRange[0]) > 0){
			if(hash.compareTo(replicaRange[0]) > 0 &&
					hash.compareTo(replicaRange[1]) < 0){
				System.out.println("isDataInReplicaRanges( ): " + key + " is in replicaOne range (case 1) on server " + serverNode.getNodeHostPort());
				return true;
			}
		}
		else{ //in this case, replicaOneRange[0] > replicaOneRange[1]
			if(hash.compareTo(replicaRange[0]) > 0){
				System.out.println("isDataInReplicaRanges( ): " + key + " is in replicaOne range (case 2) on server " + serverNode.getNodeHostPort());
				return true;
			}
		}
		return false;
	}

	private boolean isDataInReplicaRanges(String key){

		BigInteger hash = null;
		try {
			 hash = getMD5(key);
		}catch (Exception e){
			e.printStackTrace();
		}
		BigInteger[] replicaOneRange = serverNode.getReplicaOneRange();
		BigInteger[] replicaTwoRange = serverNode.getReplicaTwoRange();
		boolean retVal;

		if(replicaOneRange != null){
			retVal =  isHashInReplicaRange(key, hash, replicaOneRange);
			if(retVal){
				return retVal;
			}
		}

		if(replicaTwoRange != null){

			retVal =  isHashInReplicaRange(key, hash, replicaTwoRange);
			if(retVal){
				return retVal;
			}
		}
		return false;

	}
	private void HandleRequest(Message msg){

        if(msg == null){
            return;
        }
		if(msg.getStatus() == StatusType.CLOSE_REQ){
			// System.out.println(serverNode.getNodeHostPort() + " starts to process CLOSE message\n");
			processMessage(msg);
            zookeeperMetaData.closeZk();
		}
		else if(isKeyInValidRange(msg.getKey())) {
			// System.out.println(serverNode.getNodeHostPort() + " starts to process message\n");
			processMessage(msg);
		}
		else if(isDataInReplicaRanges(msg.getKey()) && msg.getStatus() == StatusType.GET){

			processMessage(msg);
		}
		else{
			Metadata metadata = null;
			System.out.println(serverNode.getNodeHostPort() + " finds msg out of range\n");
			try {
				metadata = zookeeperMetaData.getMetadata();
			}catch (KeeperException | InterruptedException e){
				e.printStackTrace();
			}
			RespondMsg(msg, StatusType.SERVER_NOT_RESPONSIBLE, metadata);
			System.out.println("SERVER_NOT_RESPONSIBLE sent\n");
		}
	}

	public void run() {
	        Message latestMsg = new Message();
			while (isOpen) {

                    try {
                        latestMsg = transmission.receiveMessage(clientSocket);

                        /* connection either terminated by the client or lost due to
                         * network problems*/
                    } catch (IOException ioe) {
                        LOGGER.error("Error: " + serverNode.getNodeHostPort() + " Connection lost with client " + this.clientId);
                        ioe.printStackTrace();
                        try {
                            if (clientSocket != null) {
                                clientSocket.close();
                                isOpen = false;
                            }
                        } catch (IOException ie) {
                            LOGGER.error("Error! Unable to tear down connection for client: " + this.clientId, ioe);
                        }
                    }
                    ServerStatusType curr_nodeStatus = serverNode.getServerStatus().getStatus();
                    if (curr_nodeStatus == ServerStatusType.INITIALIZE ||
                            curr_nodeStatus == ServerStatusType.IDLE) {

                        RespondMsg(latestMsg, StatusType.SERVER_STOPPED, null);
                        continue;
                    } else if (curr_nodeStatus == ServerStatusType.RUNNING) {
                        // System.out.println(serverNode.getNodeHostPort() + ": ClientConnection reached running\n");
                        HandleRequest(latestMsg);
                        continue;
                    } else if (curr_nodeStatus == ServerStatusType.MOVE_DATA_SENDER ||
                            curr_nodeStatus == ServerStatusType.MOVE_DATA_RECEIVER) {
                        if(latestMsg != null) {
                            if (latestMsg.getStatus() == KVMessage.StatusType.PUT) {
                                RespondMsg(latestMsg, StatusType.SERVER_WRITE_LOCK, null);
                            } else {
                                HandleRequest(latestMsg);
                            }
                        }
                    } else if (curr_nodeStatus == ServerStatusType.CLOSE) {
                        close();
                    }
            }
		}


	private void close(){
		try {
			clientSocket.close();
			System.out.println("ClientConnection Thread: server socket is closed now\n");
		} catch (IOException e) {
			LOGGER.error("Cannot close socket in Client Connection ");
		}
		isOpen = false;
	}

	private String[] getSuccessors(){

		Metadata metadata = null;
		try {
			metadata = zookeeperMetaData.getMetadata();
		}catch (KeeperException | InterruptedException e){
			e.printStackTrace();
		}
		String[] successors = new String[2];
		successors[0] = metadata.findSuccessor(serverNode.getNodeHostPort());
		if(successors[0] != null){
			System.out.println("getSuccessors(): " + serverNode.getNodeHostPort() + " successor 1 is " + successors[0] + "\n");
			successors[1] = metadata.findSuccessor(successors[0]);
			if(successors[1] != null){
				System.out.println("getSuccessors(): " + serverNode.getNodeHostPort() + " successor 2 is " + successors[1] + "\n");
			}
			else{
				System.out.println("getSuccessors(): " + serverNode.getNodeHostPort() + " successor 2 is not found\n");
			}
		}
		else{
			System.out.println("getSuccessors(): " + serverNode.getNodeHostPort() + " no successors are found\n");
			//reset successors
			successors[0] = null;
			successors[1] = null;
		}
		return successors;
	}

	private void sendMsgToReplica(Message msg, String hostIP, int replicaNum){

		String[] temp = hostIP.split(":");
		int port = Integer.parseInt(temp[1]) + REPLICATION_PORT_OFFSET;
		System.out.println(serverNode.getNodeHostPort() + " is sending replica to localhost:" + Integer.toString(port));
		kvStore = new KVStore(temp[0], port);
		try {
			kvStore.connect();
			kvStore.put(replicaNum, 0, msg.getKey(), msg.getValue());

			//printMessage(msg);
		}catch (IOException e){
			e.printStackTrace();
		}
		kvStore.disconnect();
	}

	private void processMessage(Message msg) {
			if(msg == null) {
				LOGGER.error("Message received is null");
				return;
			}
			/*
			 * we never did seq numbers, but we talked about it in the report so...
			 * 'error handling stuff'
			 */
			if (seqIdValues.contains(msg.getSeq())) {
					// already seen this message
					LOGGER.debug("Duplicate message with seq " + msg.getSeq() + " from client " + this.clientId);
					LOGGER.debug(gson.toJson(msg));
					return;
				}
			seqIdValues.add(msg.getSeq());

			Message return_msg = null;
			if(msg.getStatus() == KVMessage.StatusType.CLOSE_REQ){
				// LOGGER.info("Client is closed, server will be closed!");
				System.out.println("Client is closed, server " + serverNode.getNodeHostPort() + " will be closed!");
				try{
					clientSocket.close();
					isOpen = false;
				}catch(IOException ie){
					LOGGER.error("Failed to close server socket!!");
				}
				return;
			}
			else if (msg.getStatus() == KVMessage.StatusType.PUT) {
				//update local changes now
				return_msg = handlePut(msg);
				transmission.sendMessage(toByteArray(gson.toJson(return_msg)), clientSocket);

				String[] successors = getSuccessors();
				if(successors[0] != null) {
					System.out.println(serverNode.getNodeHostPort() + ": send msg to the 1st sucessor\n");
					sendMsgToReplica(msg, successors[0], 1);

					if (successors[1] != null) {
						System.out.println(serverNode.getNodeHostPort() + ": send msg to the 2nd sucessor\n");
						sendMsgToReplica(msg, successors[1], 2);
					}
				}
				else{
					System.out.println(serverNode.getNodeHostPort() + ": no sucessors to send\n");
				}
				return;
			} else {
				return_msg = handleGet(msg);
			}

			// System.out.println(serverNode.getNodeHostPort() + " : about to send message (" + msg.getKey() + "," + msg.getValue() + ")\n");
			boolean success = transmission.sendMessage(toByteArray(gson.toJson(return_msg)), clientSocket);
			if (!success) {
				LOGGER.error("Send message failed to client " + this.clientId);
			}
	}

	private boolean checkValidkey(String key) {
		if (key != null && !(key.isEmpty()) && !(key.equals("")) && !(key.contains(" ")) && !(key.length() > 20)){
			return true;
		}

		return false;
	}

	private Message handlePut (Message msg) {
		Message return_msg = null;
		String key = msg.getKey();
		String value = msg.getValue();
		
		if(checkValidkey(key) && (value.length() < 120)){
			LOGGER.info("valid key " + key);
			if(value == null || value.isEmpty()){
				LOGGER.info("Interpreted as a delete for key: " + key);
				boolean success = storageManager.deleteRV(key);
				LOGGER.info("DELETE_SUCCESS: <" + msg.getKey() + "," + storageManager.getKV(msg.getKey()) + ">");
				return_msg = new Message(StatusType.DELETE_SUCCESS, msg.getClientID(), msg.getSeq(), msg.getKey(), value);
				if(!success){
					LOGGER.info("DELETE_ERROR: <" + key+ "," + storageManager.getKV(key) + ">");
					return_msg = new Message(StatusType.DELETE_ERROR, msg.getClientID(), msg.getSeq(), key, value);
				}
			} else{
				LOGGER.info("Interpreted as a put for key: " + key);
				if(storageManager.doesKeyExist(key)){
					storageManager.putKV(key, value);
					LOGGER.info("PUT_UPDATE: <" + msg.getKey() + "," + msg.getValue() + ">");
					return_msg = new Message(StatusType.PUT_UPDATE, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
				}
				else{
					storageManager.putKV(key, value);
					LOGGER.info("PUT_SUCCESS: <" + msg.getKey() + "," + msg.getValue() + ">");
					return_msg = new Message(StatusType.PUT_SUCCESS, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
				}
			}
		}
		else{
			LOGGER.info("PUT_ERROR: <" + msg.getKey() + "," + msg.getValue() + ">");
			return_msg = new Message(StatusType.PUT_ERROR, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
		}
		return return_msg;
	}

	private Message handleGet(Message msg){
		
		Message return_msg = null;
		
		String key = msg.getKey();
		String newValue = storageManager.getKV(key);
		
		if (checkValidkey(key)) {
			if(newValue != null && !(newValue.isEmpty())){
				LOGGER.info("GET_SUCCESS: <" + msg.getKey() + "," + newValue + ">");
				return_msg = new Message(StatusType.GET_SUCCESS, msg.getClientID(), msg.getSeq(), msg.getKey(), newValue);
			} else{
				LOGGER.info("GET_ERROR" + ": <" + msg.getKey() + ",null>");
				return_msg = new Message(StatusType.GET_ERROR, msg.getClientID(), msg.getSeq(), msg.getKey(), null);
			}
		} else {
			LOGGER.info("GET_ERROR" + ": <" + msg.getKey() + ",null>");
			return_msg = new Message(StatusType.GET_ERROR, msg.getClientID(), msg.getSeq(), msg.getKey(), null);
		}

		return return_msg;
	}

	//temp use for debugging
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

	private byte[] toByteArray(String s) {
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];

		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

		return tmp;
	}




}
