package app_kvServer;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
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

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {
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

	/**
	 * Constructs a new ClientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, ServerNode node ,StorageManager caching, int clientId, String zookeeperHost, int sessionTimeout) {
		this.clientSocket = clientSocket;
		this.clientId = clientId;
		// initialized when declared above
//		this.isOpen = true;
//		this.transmission = new Transmission();
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

        Message return_msg = new Message(responseType, clientId, received_msg.getSeq(), received_msg.getKey(), received_msg.getValue());
        String metadata = gson.toJson(metaData);
        return_msg.setMetaData(metadata);

        System.out.println(gson.toJson(return_msg));
        boolean success = transmission.sendMessage(toByteArray(gson.toJson(return_msg)), clientSocket);
	    if (!success) {
	        LOGGER.error("Send message failed to client " + this.clientId);
            //System.out.println("Send message failed to client " + this.clientId);
	    }
    }


//    private void HandleRequest(Message msg){
//        if(isKeyInValidRange(msg.getKey())) {
//            processMessage(msg);
//        }
//        else{
//            Metadata metadata = null;
////            try {
////                metadata = zookeeperMetaData.getMetadata();
////            }catch (KeeperException | InterruptedException e){
////                e.printStackTrace();
////            }
//            metadata = new Metadata();
//            String serverIpPort = "100.64.193.243:60000";
//            metadata.addServer(serverIpPort);
//            BigInteger[] range = new BigInteger[2];
//            try {
//                range[0] = getMD5("a");
//                range[1] = getMD5("z");
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//
//            serverNode.setRange(range);
//            RespondMsg(msg, StatusType.SERVER_NOT_RESPONSIBLE, metadata);
//            System.out.println("SERVER_NOT_RESPONSIBLE sent\n");
//        }
//    }

    private void HandleRequest(Message msg){
        if(isKeyInValidRange(msg.getKey())) {
            processMessage(msg);
        }
        else{
            Metadata metadata = null;
            try {
                metadata = zookeeperMetaData.getMetadata();
            }catch (KeeperException | InterruptedException e){
                e.printStackTrace();
            }
            RespondMsg(msg, StatusType.SERVER_NOT_RESPONSIBLE, metadata);
            System.out.println("SERVER_NOT_RESPONSIBLE sent\n");
        }
    }

	/**
	 * Initializes and starts the client connection.
	 * Loops until the connection is closed or aborted by the client.
	 */
	@Override
	public void run() {
		Message latestMsg;
        System.out.printf("server that accepts the client connection starts running...");
		while (isOpen) {
			try {
				latestMsg = transmission.receiveMessage(clientSocket);
				if (serverNode.getServerStatus().getStatus() == ServerStatusType.INITIALIZE ||
						serverNode.getServerStatus().getStatus() == ServerStatusType.IDLE) {
					RespondMsg(latestMsg, StatusType.SERVER_STOPPED, null);
				} else if (serverNode.getServerStatus().getStatus() == ServerStatusType.RUNNING) {
					HandleRequest(latestMsg);
				} else if (serverNode.getServerStatus().getStatus() == ServerStatusType.MOVE_DATA_SENDER ||
						serverNode.getServerStatus().getStatus() == ServerStatusType.MOVE_DATA_RECEIVER) {
					if (latestMsg.getStatus() == KVMessage.StatusType.PUT) {
						RespondMsg(latestMsg, StatusType.SERVER_WRITE_LOCK, null);
					} else {
						HandleRequest(latestMsg);
					}
				} else if (serverNode.getServerStatus().getStatus() == ServerStatusType.CLOSE) {
					close();
				}
				Thread.sleep(1);
				/* connection either terminated by the client or lost due to
				 * network problems*/
			}
			catch(SocketTimeoutException e){
				System.out.println(serverNode.getServerStatus().getStatus().name());
				if (serverNode.getServerStatus().getStatus() == ServerStatusType.CLOSE) {
					close();
				}
			} catch (IOException ioe) {
				LOGGER.error("Error! Connection lost with client " + this.clientId);
				ioe.printStackTrace();
				try {
					if (clientSocket != null) {
						clientSocket.close();
						isOpen = false;
					}
				} catch (IOException ie) {
					LOGGER.error("Error! Unable to tear down connection for client: " + this.clientId, ioe);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	private void close(){
		try {
			clientSocket.close();
		} catch (IOException e) {
			LOGGER.error("Cannot close socket in Client Connection ");
		}
		isOpen = false;
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
				LOGGER.info("Client is closed, server will be closed!");
				try{
					clientSocket.close();
					isOpen = false;
				}catch(IOException ie){
					LOGGER.error("Failed to close server socket!!");
				}
				return;
			}
			else if (msg.getStatus() == KVMessage.StatusType.PUT) {
				return_msg = handlePut(msg);
			} else {
				return_msg = handleGet(msg);
			}

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
