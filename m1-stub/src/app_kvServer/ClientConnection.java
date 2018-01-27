package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import com.google.gson.Gson;
import common.cache.CacheManager;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.Message;
import common.transmission.Transmission;
import org.apache.log4j.Logger;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private final static Logger LOGGER = Logger.getLogger(ClientConnection.class);
    private final int TIMEOUT = 1000000; // milliseconds
	private boolean isOpen;
	private final static Gson gson = new Gson();
	private CacheManager CacheManager;
	private Socket clientSocket;
	private Transmission transmission;
	private int clientId;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 *
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, CacheManager caching, int clientId) {
		this.clientSocket = clientSocket;
		this.clientId = clientId;
		this.isOpen = true;
		this.transmission = new Transmission();
		this.CacheManager = caching;

		try {
			clientSocket.setSoTimeout(TIMEOUT);
		}catch (SocketException e) {
			e.printStackTrace();
		}
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
				isOpen = false;
				ioe.printStackTrace();
				try {
					if (clientSocket != null) {
						clientSocket.close();
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

			Message return_msg;
			if (msg.getStatus() == KVMessage.StatusType.PUT) {
				return_msg = handlePut(msg);
			} else {
				return_msg = handleGet(msg);
			}

			boolean success = transmission.sendMessage(toByteArray(gson.toJson(return_msg)), clientSocket);
			if (!success) {
				LOGGER.error("Send message failed to client " + this.clientId);
			}
	}

	private boolean isDelete(String value) {
		if (value == null || value.isEmpty()){
            LOGGER.info("Interpreted as a DELETE request; value = "+value);
			return true;
		}
		return false;
	}

	private Message handlePut (Message msg) {
		Message return_msg;
		if (CacheManager.doesKeyExist(msg.getKey())) {

			if (isDelete(msg.getValue())) {
				if (CacheManager.deleteRV(msg.getKey())) {
					LOGGER.info("DELETE_SUCCESS: <" + msg.getKey() + "," + CacheManager.getKV(msg.getKey()) + ">");
					return_msg = new Message(StatusType.DELETE_SUCCESS, msg.getClientID(), msg.getSeq(), msg.getKey(), CacheManager.getKV(msg.getKey()));
				} else {
					LOGGER.info("DELETE_ERROR: <" + msg.getKey() + "," + CacheManager.getKV(msg.getKey()) + ">");
					return_msg = new Message(StatusType.DELETE_ERROR, msg.getClientID(), msg.getSeq(), msg.getKey(), CacheManager.getKV(msg.getKey()));
				}
			} else {
				if (CacheManager.putKV(msg.getKey(), msg.getValue())) {
					LOGGER.info("PUT_UPDATE: <" + msg.getKey() + "," + msg.getValue() + ">");
					return_msg = new Message(StatusType.PUT_UPDATE, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
				} else {
					LOGGER.info("PUT_ERROR: <" + msg.getKey() + "," + msg.getValue() + ">");
					return_msg = new Message(StatusType.PUT_ERROR, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
				}
			}

		} else {

            if (isDelete(msg.getValue())) {
				LOGGER.info("DELETE_ERROR: <" + msg.getKey() + ", null >");
				return_msg = new Message(StatusType.DELETE_ERROR, msg.getClientID(), msg.getSeq(), msg.getKey(), null);
			} else{
				if (CacheManager.putKV(msg.getKey(), msg.getValue())) {
					LOGGER.info("PUT_SUCCESS: <" + msg.getKey() + "," + msg.getValue() + ">");
					return_msg = new Message(StatusType.PUT_SUCCESS, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
				} else {
					LOGGER.info("PUT_ERROR: <" + msg.getKey() + "," + msg.getValue() + ">");
					return_msg = new Message(StatusType.PUT_ERROR, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
				}
			}
		}
		return return_msg;
	}

	private Message handleGet(Message msg){
		Message return_msg;
		String value = CacheManager.getKV(msg.getKey());
		if (value != null) {
			LOGGER.info("GET_SUCCESS: <" + msg.getKey() + "," + value + ">");
			return_msg = new Message(StatusType.GET_SUCCESS, msg.getClientID(), msg.getSeq(), msg.getKey(), value);
		} else {
			LOGGER.info(msg.getStatus() + ": <" + msg.getKey() + ",null>");
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
