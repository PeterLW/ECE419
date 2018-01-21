package client;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import common.messages.KVMessage;
import org.apache.log4j.Logger;

import common.transmission.Transmission;

public class KVStore implements KVCommInterface {


	private Logger logger = Logger.getRootLogger();
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private boolean running;
	private static int client_id = 0;

	private Transmission transmit;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	/**
	 * Initialize KVStore with address and port of KVServer
	 *
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		clientSocket = new Socket(address, port);
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		client_id = client_id++;
		transmit  = new Transmission();
	}

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		try{
			setRunning(true);
			logger.info("Connection established");
		} catch (IOException ioe) {
			logger.error("Unable to connect!");
		}
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		try{
			setRunning(false);
			logger.info("tearing down the connection ...");
			if (clientSocket != null){
				clientSocket.close();
				clientSocket = null;
				logger.info("connection closed!");
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
}

	@Override
	public /*KVMessage*/String put(String key, String value) throws Exception {
		// TODO Auto-generated method stub

		if(isRunning()) {
			try {
				StringBuilder send_msg = new StringBuilder();
				send_msg.append("PUT");
				send_msg.append(Integer.toString(client_id));
				send_msg.append(key);
				send_msg.append(value);
				transmit.sendMessage(toByteArray(send_msg), output);

				byte[] status = transmit.receiveMessage(input);

			} catch (IOException ioe) {
				if(isRunning()) {
					logger.error("Connection lost!");
					try {
						disconnect();
					} catch (IOException e) {
						logger.error("Unable to close connection!");
					}
				}
			}
		}
		String ret_val = new String(status).trim();
		return ret_val;
	}

	@Override
	public /*KVMessage*/String get(String key) throws Exception {
		// TODO Auto-generated method stub
		if(isRunning()) {
			try {
				StringBuilder send_msg = new StringBuilder();
				send_msg.append("GET");
				send_msg.append(Integer.toString(client_id));
				send_msg.append(key);
				send_msg.append("");
				transmit.sendMessage(toByteArray(send_msg), output);

				byte[] value = transmit.receiveMessage(input);

			} catch (IOException ioe) {
				if(isRunning()) {
					logger.error("Connection lost!");
					try {
						disconnect();
					} catch (IOException e) {
						logger.error("Unable to close connection!");
					}
				}
			}
		}
		String ret_val = new String(value).trim();
		return ret_val;//note: for debugging, later needs to be changed to KVMessage type.
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean run) {
		running = run;
	}
	//temp use for debugging
	private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];

		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

		return tmp;
	}
}
