package client;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.jcraft.jsch.IO;
import common.messages.Message;
import common.metadata.Metadata;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.transmission.Transmission;
import common.messages.KVMessage.StatusType;


public class KVStore implements KVCommInterface {

	private final static Logger LOGGER = Logger.getLogger(KVStore.class);
	private final int TIMEOUT = 10000; // idk set this later - nanoseconds
	private final Gson gson = new Gson();

	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private boolean running;
	private int clientId = 0;
	private int seqNum = 0;

	private String address;
	private int port;

	private Message message = null;
	private Transmission transmit;

	private Metadata metadata = null;

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
		this.metadata = new Metadata();
	}

	@Override
	public void connect() throws IOException{
		// TODO Auto-generated method stub

        clientSocket = new Socket(address, port);
		clientSocket.setSoTimeout(TIMEOUT);
		this.output = clientSocket.getOutputStream();
		this.input = clientSocket.getInputStream();
        setRunning(true);

        String initialMessage = this.transmit.receiveMessageString(clientSocket); // should be clientId
		this.clientId = Integer.parseInt(initialMessage);

        // LOGGER.info("Connection established, client_id: " + this.clientId);
        System.out.println("Connection established, client_id: " + this.clientId);
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		try{
			setRunning(false);
			// LOGGER.info("tearing down the connection ...");
			System.out.println("tearing down the connection ...");
			
			message = new Message(StatusType.CLOSE_REQ, clientId, seqNum, "");
			transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);
			
			if (clientSocket != null){
				clientSocket.close();
				//LOGGER.info("connection closed!");
				System.out.println("client connection closed\n");
			}
		} catch (IOException ioe) {
			LOGGER.error("Unable to close connection!");
		}
	}

	private void disconnect(String key) {
		// TODO Auto-generated method stub
		try{
			setRunning(false);
			System.out.println("tearing down the connection ...");
			
			message = new Message(StatusType.CLOSE_REQ, clientId, seqNum, key);
			transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);

//			try{
//				Thread.sleep(TIMEOUT);
//			}
//			catch(InterruptedException e){
//				e.printStackTrace();
//			}

			if (clientSocket != null){
				clientSocket.close();
				LOGGER.info("connection closed!");
			}

		} catch (IOException ioe) {
			LOGGER.error("Unable to send message!\n");
		}
		
	}

	@Override
	public boolean isConnected() {
		// TODO Auto-generated method stub
		return false;
	}

	public void printmetadata(){
		System.out.println("printing metadata");
		LinkedHashMap<BigInteger, String> md = metadata.getMetadata();
		for (Map.Entry<BigInteger,String> entry : md.entrySet()){
			String serverNodeName = entry.getValue();
			System.out.println(serverNodeName);
		}
	}

	public void updateMetadataAndResend(Message msg, String key, String value) throws IOException{
		
		if(msg == null){
			return;
		}
		if(msg.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE){
            metadata = gson.fromJson(msg.getMetaData(), Metadata.class);
            //update bst, so checkValidServer will pass
            metadata.build_bst();
           // printmetadata();
			put(key,value);
		}
	}

	public boolean checkValidServer(String key){
		String targetServer = metadata.findServer(key);
		if(targetServer == null)
			return true;

		String[] temp = targetServer.split(":");
		if(this.address.equals(temp[0]) && this.port == Integer.parseInt(temp[1]))
			return true;
		else {
			this.address = temp[0];
			this.port = Integer.parseInt(temp[1]);
			return false;
		}
	}

	public Message put(int ClientID, int seqNum, String key, String value) throws IOException {

		if(!checkValidServer(key)) {
			System.out.println("client needs to  reconnect to " + this.port +", needs to disconnect " +"\n");

			disconnect(key);
			connect();
		}

		Message received_stat = null;
		boolean isTimeOut = false;

		if (isRunning()) {
			message = new Message(StatusType.PUT, ClientID, seqNum, key, value);
			transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);

			// the SoTTimeout is already set in constructor
			try {
				received_stat = transmit.receiveMessage(clientSocket); // receive reply, note receiveMessage( ) is a blocking function
				//System.out.println(gson.toJson(received_stat));
			} catch (java.net.SocketTimeoutException e) {// read timed out - you may throw an exception of your choice
				isTimeOut = true;
			}

			if (isTimeOut) { // try again once
				LOGGER.debug("Timeout: PUT message failed - Trying again");
				// you don't have to make a new message, the message object from above is still in scope.
				transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);
				clientSocket.setSoTimeout(TIMEOUT + 10000); // doubles the timeout time
				try {
					received_stat = transmit.receiveMessage(clientSocket); // receive reply, note receiveMessage( ) is a blocking function
					isTimeOut = false;
				} catch (java.net.SocketTimeoutException e) {
					// read timed out - you may throw an exception of your choice
					isTimeOut = true;
				}
				clientSocket.setSoTimeout(TIMEOUT); // resets to original
			}

			if (!isTimeOut && received_stat != null) {
				LOGGER.info(gson.toJson(message));
				return received_stat;
			} else{
				LOGGER.error("Timeout: PUT message failed");
				return null;
			}

		} else {
			LOGGER.error("Connection lost: PUT message failed");
		}
		return null;
	}

	@Override
	public Message put(String key, String value) throws IOException {

		if(!checkValidServer(key)) {
			System.out.println("client needs to  reconnect to " + this.port +", needs to disconnect " +"\n");
	
			disconnect(key);
			connect();
		}

		Message received_stat = null;
		boolean isTimeOut = false;

		if (isRunning()) {
			message = new Message(StatusType.PUT, clientId, seqNum, key, value);
			transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);

			seqNum++;
			// the SoTTimeout is already set in constructor
			try {
				received_stat = transmit.receiveMessage(clientSocket); // receive reply, note receiveMessage( ) is a blocking function
				//System.out.println(gson.toJson(received_stat));
			} catch (java.net.SocketTimeoutException e) {// read timed out - you may throw an exception of your choice
				isTimeOut = true;
			}

			if (isTimeOut) { // try again once
				LOGGER.debug("Timeout: PUT message failed - Trying again");
				// you don't have to make a new message, the message object from above is still in scope.
				transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);
				clientSocket.setSoTimeout(TIMEOUT + 10000); // doubles the timeout time
				try {
					received_stat = transmit.receiveMessage(clientSocket); // receive reply, note receiveMessage( ) is a blocking function
					isTimeOut = false;
				} catch (java.net.SocketTimeoutException e) {
					// read timed out - you may throw an exception of your choice
					isTimeOut = true;
				}
				clientSocket.setSoTimeout(TIMEOUT); // resets to original
			}

			if (!isTimeOut && received_stat != null) {
				LOGGER.info(gson.toJson(message));
				return received_stat;
			} else{
				LOGGER.error("Timeout: PUT message failed");
				return null;
			}

		} else {
			LOGGER.error("Connection lost: PUT message failed");
		}
		return null;
	}

	@Override
	public Message get(String key) throws Exception {

        if(!checkValidServer(key)) {
            clientSocket.close();
            connect();
        }

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
				LOGGER.debug("timeout");
				isTimeOut = true;
			}

			if (isTimeOut) { // try again once
				transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);
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
