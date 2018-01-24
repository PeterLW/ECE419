package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import com.google.gson.Gson;
import common.cache.Cache;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.Message;
import org.apache.log4j.*;
import common.messages.Message;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.transmission.Transmission;
import com.google.gson.Gson;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	private boolean isOpen;
	private Gson gson = null;
	private Cache cache;
	private Socket clientSocket;
	private Transmission transmission;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 *
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, Cache caching) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.transmission = new Transmission();
		this.gson = new Gson();
		this.cache = caching;
	}

	/**
	 * Initializes and starts the client connection.
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {

			while (isOpen) {
				try {
					Message latestMsg = transmission.receiveMessage(clientSocket);
//					byte[] latestMsg = transmission.receiveMessage(clientSocket);
					msg_process(latestMsg);
				/* connection either terminated by the client or lost due to
				 * network problems*/
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}
			}
			try {
				if (clientSocket != null) {
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
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

	private void msg_process(Message msg) {

			if(msg != null) {

				Message return_msg = null;
				if (msg.getStatus() == KVMessage.StatusType.PUT) {

					if (cache.in_cache(msg.getKey()) == true) {

						if (cache.putKV(msg.getKey(), msg.getValue()) == true) {
							logger.info("PUT_UPDATE: <" + msg.getKey() + "," + msg.getValue() + ">");
							return_msg = new Message(StatusType.PUT_UPDATE, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
						} else {
							logger.info("PUT_ERROR: <" + msg.getKey() + "," + msg.getValue() + ">");
							return_msg = new Message(StatusType.PUT_ERROR, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
						}
					} else {
						if (cache.putKV(msg.getKey(), msg.getValue()) == true) {
							logger.info("PUT_SUCCESS: <" + msg.getKey() + "," + msg.getValue() + ">");
							return_msg = new Message(StatusType.PUT_SUCCESS, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
						} else {
							logger.info("PUT_ERROR: <" + msg.getKey() + "," + msg.getValue() + ">");
							return_msg = new Message(StatusType.PUT_ERROR, msg.getClientID(), msg.getSeq(), msg.getKey(), msg.getValue());
						}
					}
				} else {

					String value = cache.getKV(msg.getKey());
					if (value != null) {
						logger.info("GET_SUCCESS: <" + msg.getKey() + "," + value + ">");
						return_msg = new Message(StatusType.GET_SUCCESS, msg.getClientID(), msg.getSeq(), msg.getKey(), value);
					} else {
						logger.info("GET_ERROR: <" + msg.getKey() + ",null>");
						return_msg = new Message(StatusType.GET_ERROR, msg.getClientID(), msg.getSeq(), msg.getKey(), null);
					}
				}
				Gson gson = new Gson();
				boolean success = transmission.sendMessage(toByteArray(gson.toJson(return_msg)), clientSocket);
				if (!success) {
					System.out.println("Send message failed");
				}
			}
			else{
				logger.info("message received is null");
			}

	}
}
