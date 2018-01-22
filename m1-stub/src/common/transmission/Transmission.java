package common.transmission;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

public class Transmission{

    private Logger logger = Logger.getRootLogger();
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    OutputStream output;
    InputStream input;

    public Transmission() {

    }

//    public void sendMessage(byte[] msg, OutputStream output) throws IOException {
    public void sendMessage(byte[] msg, Socket socket) throws IOException {
        byte[] msgBytes = msg;
        output = socket.getOutputStream();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("Send message:\t <" + new String(msg) + ">");
    }

//    public byte[] receiveMessage(InputStream input) throws IOException {
    public byte[] receiveMessage(Socket socket) throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        input = socket.getInputStream();
        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(read != 13 && reading) {/* carriage return */
            /* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and numbers */
            if((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

            /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){ //if message shorter than buffersize, bufferBytes won't be conpied to msgBytes
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else { //after exiting while loop, fetch last portion of data in bufferBytes
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

        logger.info("Receive message:\t <" + new String(msgBytes) + ">");
        return msgBytes;
    }
}