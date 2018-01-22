package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import common.cache.Cache;
import org.apache.log4j.*;

import common.transmission.Transmission;


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
	private  Cache cache;
	private Socket clientSocket;
	private Transmission transmission;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, Cache caching) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		transmission = new Transmission();
		cache = caching;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			while(isOpen) {
				try {
					byte[] latestMsg = transmission.receiveMessage(clientSocket);
					msg_process(latestMsg);
					//transmission.sendMessage(latestMsg, clientSocket);

				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
		} catch (Exception ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			try {
				if (clientSocket != null) {
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	//temp use for debugging
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

	private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];

		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

		return tmp;
	}

	private void msg_process(byte[] msg){
		String new_msg = new String(msg);
		String[] tokens = new_msg.split("\\.");

		try {
			if (tokens[0].equals("PUT")) {
				transmission.sendMessage(toByteArray("PutSucess"), clientSocket);
				cache.putKV(tokens[2], tokens[3]);
			}
			if (tokens[0].equals("GET")) {
				String value = cache.getKV(tokens[2]);
				System.out.println("GET SUCCESS");
				transmission.sendMessage(toByteArray(value), clientSocket);
			}
		}catch (IOException ioe){
			logger.error("Msg processor error!");
		}
	}
	
}
