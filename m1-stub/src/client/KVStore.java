package client;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.google.gson.Gson;
import common.messages.Message;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.transmission.Transmission;
import common.messages.KVMessage.StatusType;


public class KVStore implements KVCommInterface {

	private static final Logger LOGGER = Logger.getRootLogger();
	private final int TIMEOUT = 60000000; // idk set this later - nanoseconds

	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private boolean running;
	private static int clientId = 0;
	private int seqNum = 0;

	String address;
	int port;

	private Message message = null;
	private Transmission transmit;
	private Gson gson = null;

	static {
		try {
			new LogSetup("logs/application.log", Level.INFO);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

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
		this.clientId = clientId++;
		this.transmit = new Transmission();
		this.gson = new Gson();
	}

	@Override
	public void connect() throws Exception{
		// TODO Auto-generated method stub
        clientSocket = new Socket(address, port);
		this.output = clientSocket.getOutputStream();
		this.input = clientSocket.getInputStream();
        setRunning(true);
        LOGGER.info("Connection established");
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

	public boolean clear(String device){
		if(device.equals("cache")){
			return true; // ??
		}
		return false;
	}

	@Override
	public Message put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		Message received_stat = null;
		boolean finish = false;

		if(isRunning()) {
			message = new Message(StatusType.PUT, clientId, seqNum, key, value);
			transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);
			//transmit.sendMessage(message), clientSocket);
			seqNum++;

			clientSocket.setSoTimeout((int)TIMEOUT);
			try {
				received_stat = transmit.receiveMessage(clientSocket); // receive reply, note receiveMessage( ) is a blocking function
				finish = true;

			} catch (java.net.SocketTimeoutException e) {
				// read timed out - you may throw an exception of your choice
				finish = false;

			}finally {

				if(received_stat != null && finish == true){
					LOGGER.info(gson.toJson(message));
					return received_stat;
				}
				else{
					LOGGER.error("Timeout: PUT message failed");
				}
			}
		}
		else{
			LOGGER.error("Connection lost: PUT message failed");
		}
		return null;
	}

	@Override
	public Message get(String key) throws Exception {
		// TODO Auto-generated method stub
		Message received_stat=null;
		boolean finish = false;

		if(isRunning()) {
			message = new Message(StatusType.GET, clientId, seqNum, key,null);
			transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);
			seqNum++;

			clientSocket.setSoTimeout((int)TIMEOUT);
			try {
				received_stat = transmit.receiveMessage(clientSocket); // receive reply, note receiveMessage( ) is a blocking function
				finish = true;

			} catch (java.net.SocketTimeoutException e) {
				// read timed out - you may throw an exception of your choice
				finish = false;

			}finally {

				if(received_stat != null && finish == true){
					LOGGER.info(gson.toJson(received_stat));
					return received_stat;
				}
				else{
					LOGGER.error("Timeout: GET message failed");
				}
			}
		}
		else{
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
