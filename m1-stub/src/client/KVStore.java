package client;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

import com.google.gson.Gson;
import common.messages.Message;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.transmission.Transmission;
import common.messages.KVMessage.StatusType;


public class KVStore implements KVCommInterface {

	private final static Logger LOGGER = Logger.getLogger(KVStore.class);
	private final int TIMEOUT = 10000; // milliseconds

	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private boolean running;
	private int clientId = 0;
	private int seqNum = 0;

	String address;
	int port;

	private Message message = null;
	private Transmission transmit;
	private final Gson gson = new Gson();

	/**
	 * Initialize KVStore with address and port of KVServer
	 *
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	public KVStore(String address, int port){
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
		this.transmit = new Transmission();
	}

	@Override
	public void connect() throws Exception{
		// TODO Auto-generated method stub
		clientSocket = new Socket(address, port);
		clientSocket.setSoTimeout(TIMEOUT);
		this.output = clientSocket.getOutputStream();
		this.input = clientSocket.getInputStream();
        	setRunning(true);

        String initialMessage = this.transmit.receiveMessageString(clientSocket); // should be clientId
		this.clientId = Integer.parseInt(initialMessage);

        LOGGER.debug("Connection established, client_id: " + this.clientId);
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		try{
			setRunning(false);
			LOGGER.info("tearing down the connection ...");
			if (clientSocket != null){
				clientSocket.close();
				clientSocket = null;
				LOGGER.info("connection closed!");
			}
		} catch (IOException ioe) {
			LOGGER.error("Unable to close connection!");
		}
	}


	@Override
	public Message put(String key, String value){
		// TODO Auto-generated method stub
		Message received_stat = null;
		boolean isTimeOut = false;

		try {
			if (isRunning()) {
				message = new Message(StatusType.PUT, clientId, seqNum, key, value);
				transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);

				seqNum++;
				clientSocket.setSoTimeout(TIMEOUT); // doubles the timeout time
				try {
					received_stat = transmit.receiveMessage(clientSocket); // receive reply, note receiveMessage( ) is a blocking function
				} catch (java.net.SocketTimeoutException e) {// read timed out - you may throw an exception of your choice
					isTimeOut = true;
				}

				if (isTimeOut) { // try again once
					clientSocket.setSoTimeout(TIMEOUT + 10000); // doubles the timeout time
					try {
						received_stat = transmit.receiveMessage(clientSocket); // receive reply, note receiveMessage( ) is a blocking function
						LOGGER.debug("Timeout: PUT message failed - Trying again");
						isTimeOut = false;
					} catch (java.net.SocketTimeoutException e) {
						// read timed out - you may throw an exception of your choice
						isTimeOut = true;
					}
					clientSocket.setSoTimeout(TIMEOUT); // resets to original
				}

				if (!isTimeOut && received_stat != null) {
					LOGGER.info("Response from server: " + gson.toJson(received_stat));
					return received_stat;
				} else {
					LOGGER.error("Timeout: PUT message failed");
					return null;
				}

			} else {
				LOGGER.error("Connection lost: PUT message failed");
			}
		} catch(IOException se){
			se.printStackTrace();
			if (se.getClass() == SocketException.class) {
				LOGGER.error("Timeout not set ");
			} else {
				LOGGER.error("IOException");
			}
		}
		return null;
	}

	@Override
	public Message get(String key) throws Exception {
		// TODO Auto-generated method stub
		Message received_stat=null;
		boolean isTimeOut = false;

		if(isRunning()) {
			message = new Message(StatusType.GET, clientId, seqNum, key,null);
			transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);
			seqNum++;

			try {
				received_stat = transmit.receiveMessage(clientSocket); // receive reply, note receiveMessage( ) is a blocking function
			} catch (java.net.SocketTimeoutException e) {
				// read timed out - you may throw an exception of your choice
				isTimeOut = true;

			}
			if (isTimeOut) { // try again once
				clientSocket.setSoTimeout(TIMEOUT + 10000); // doubles the timeout time
				try {
					received_stat = transmit.receiveMessage(clientSocket); // receive reply, note receiveMessage( ) is a blocking function
					LOGGER.debug("Timeout: GET message failed - Trying again");
					isTimeOut = false;
				} catch (java.net.SocketTimeoutException e) {
					// read timed out - you may throw an exception of your choice
					isTimeOut = true;
				}
				clientSocket.setSoTimeout(TIMEOUT); // resets to original
			}

			if (!isTimeOut && received_stat != null) {
				LOGGER.info("Response from server: " + gson.toJson(received_stat));
				return received_stat;
			} else{
				LOGGER.error("Timeout: GET message failed");
				return null;
			}
		}else{
			LOGGER.error("Connection lost: GET message failed");
		}
		return null;
	}


	public boolean isRunning() {
		return running;
	}

	private void setRunning(boolean run) {
		running = run;
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
}
