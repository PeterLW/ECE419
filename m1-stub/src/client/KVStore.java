package client;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import common.transmission.Transmission;

public class KVStore implements KVCommInterface {


	private Logger logger = Logger.getRootLogger();
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private boolean running;
	private static int client_id = 0;

	String address;
	int port;

	private Transmission transmit;

	/**
	 * Initialize KVStore with address and port of KVServer
	 *
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	public KVStore(String address, int port){
		// TODO Auto-generated method stub
		try {
			this.address = address;
			this.port = port;
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			client_id = client_id++;
			transmit = new Transmission();
		}catch(IOException ioe){
			logger.error("Input/Outputstream initialization failed!");
		}
	}

	@Override
	public void connect() throws Exception{
		// TODO Auto-generated method stub
        clientSocket = new Socket(address, port);
        setRunning(true);
        logger.info("Connection established");
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
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		byte[] status={};

		if(isRunning()) {
            StringBuilder send_msg = new StringBuilder();
            send_msg.append("PUT");
            send_msg.append(Integer.toString(client_id));
            send_msg.append(key);
            send_msg.append(value);
            transmit.sendMessage(toByteArray(send_msg.toString()), output);

            status = transmit.receiveMessage(input);
		}
        KVMessage received_stat = new Message(new String(status));
		System.out.println(new String(status));
		return received_stat;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		byte[] value = {};

		if(isRunning()) {
            StringBuilder send_msg = new StringBuilder();
            send_msg.append("GET");
            send_msg.append(Integer.toString(client_id));
            send_msg.append(key);
            send_msg.append("");
            transmit.sendMessage(toByteArray(send_msg.toString()), output);

            value = transmit.receiveMessage(input);
		}
        KVMessage received_value = new Message(new String(value));
		System.out.println(new String(value));
		return received_value;//note: for debugging, later needs to be changed to KVMessage type.
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean run) {
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
