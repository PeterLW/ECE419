package app_kvServer;

import java.io.IOException;
import java.net.Socket;

import com.google.gson.Gson;
import common.cache.StorageManager;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.Message;
import org.apache.log4j.*;
import common.transmission.Transmission;
import java.lang.String;
import java.util.HashSet;

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
	private boolean isOpen;
	private StorageManager storageManager;
	private Socket clientSocket;
	private Transmission transmission;
	private int clientId;
	private HashSet<Integer> seqIdValues = new HashSet<Integer>();

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 *
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, StorageManager caching, int clientId) {
		this.clientSocket = clientSocket;
		this.clientId = clientId;
		this.isOpen = true;
		this.transmission = new Transmission();
		this.storageManager = caching;

		String clientIdString = Integer.toString(clientId);
		transmission.sendMessage(toByteArray(clientIdString),clientSocket);
	}


	/**
	 * Initializes and starts the client connection.
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
        Message latestMsg = new Message();
		while (isOpen) {
            try {
				 latestMsg = transmission.receiveMessage(clientSocket);

				/* connection either terminated by the client or lost due to
				 * network problems*/
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
			}
            processMessage(latestMsg);
		}
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
