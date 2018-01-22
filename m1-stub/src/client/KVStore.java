package client;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Date;

import com.google.gson.Gson;
import common.messages.KVMessage;
import common.messages.Message;
import org.apache.log4j.Logger;

import common.transmission.Transmission;
import common.messages.KVMessage.StatusType;


public class KVStore implements KVCommInterface {

	private static final Logger LOGGER = Logger.getRootLogger();
	private final long TIMEOUT = 60000000; // idk set this later - nanoseconds

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

		LOGGER.error("Input/Outputstream initialization failed!");
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

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		byte[] status={};
		long startTime = 0, timeElapsed = 0;
		int i = 0;

		if(isRunning()) {
			message = new Message(StatusType.PUT, clientId, seqNum, key, value);
			startTime = System.nanoTime();
			transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);
			seqNum++;
			timeElapsed = System.nanoTime() - startTime;
			while (timeElapsed < TIMEOUT) {
				/* right now server isn't built to send a reply, so there's always no reply
				 * this should probably resend if timeout, but until the server is built to send
				 * a reply, I don't want to add it yet, this will be an infinite loop
				 */
				if(input.available() != 0) {// nonblocking call
					status = transmit.receiveMessage(clientSocket); // receive reply
					/*
					 * I don't think it's necessary to pass in the socket?
					 * if the inputStream was null before, perhaps it was because it was created
					 * before the socket was connected?
					 */
				}

				if (i==200) {
					/* nanoTime is only accurate if called ~ms or so,
					 * so didn't want it to call too often
					 * alternative: TimeUnit.SECONDS.sleep(1)
					 */
					timeElapsed = System.nanoTime() - startTime;
					i = 0;
				}
				i++;
			}
		}

        Message received_stat = gson.fromJson(new String(status),Message.class);
		System.out.println(new String(status));
		return received_stat;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		byte[] value = {};
		long startTime = 0, timeElapsed = 0;
		int i = 0;

		if(isRunning()) {
			message = new Message(StatusType.GET, clientId, seqNum, key);
			startTime = System.nanoTime();
			transmit.sendMessage(toByteArray(gson.toJson(message)), clientSocket);
			seqNum++;

			System.out.println("test");
			timeElapsed = System.nanoTime() - startTime;
			while (timeElapsed < TIMEOUT) {
				/* right now server isn't built to send a reply, so there's always no reply
				 * this should probably resend if timeout, but until the server is built to send
				 * a reply, I don't want to add it yet, this will be an infinite loop
				 */
				System.out.println("test1");
				if(input.available() != 0) {// nonblocking call
					System.out.println("test2");
					value = transmit.receiveMessage(clientSocket); // receive reply
					/*
					 * I don't think it's necessary to pass in the socket?
					 * if the inputStream was null before, perhaps it was because it was created
					 * before the socket was connected?
					 */
				}

				if (i==200) {
					/* nanoTime is only accurate if called ~ms or so,
					 * so didn't want it to call too often
					 * alternative: TimeUnit.SECONDS.sleep(1)
					 */
					timeElapsed = System.nanoTime() - startTime;
					i = 0;
				}
				i++;
			}
		}

        Message received_value = gson.fromJson(new String(value),Message.class);
		System.out.println(new String(value));
		return received_value; //note: for debugging, later needs to be changed to KVMessage type.
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
