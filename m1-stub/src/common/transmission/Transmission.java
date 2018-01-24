package common.transmission;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import com.google.gson.Gson;
import common.messages.Message;
import common.messages.KVMessage.StatusType;
import org.apache.log4j.Logger;

public class Transmission {

    private static final Logger LOGGER = Logger.getRootLogger();
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private Gson gson = null;
    OutputStream output;
    InputStream input;

    public Transmission() {

        this.gson = new Gson();
    }

    public void sendMessage(byte[] msg, Socket socket) throws IOException {
        byte[] msgBytes = msg;
        output = socket.getOutputStream();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        LOGGER.debug("Send message:\t '" + new String(msg) + "'");
    }

//    public void sendMessage(Message msg) throws IOException {
//        byte[] msgBytes = msg.getMsgBytes();
//        output.write(msgBytes, 0, msgBytes.length);
//        output.flush();
//        LOGGER.debug("Send message:\t '" + new String(msg) + "'");
//    }

    public Message receiveMessage(Socket socket) throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];
        input = socket.getInputStream();

        byte read = (byte) input.read(); // blocks until input data available
        boolean reading = true;

        while (read != 13 && reading) {/* carriage return */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
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
            if ((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

            /* stop reading is DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if (msgBytes == null) { //if message shorter than buffersize, bufferBytes won't be conpied to msgBytes
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else { //after exiting while loop, fetch last portion of data in bufferBytes
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;
        Gson gson = new Gson();
        Message recv_msg = gson.fromJson(msgBytes.toString(), Message.class);
        LOGGER.info("Receive message:\t '" + new String(msgBytes) + "'");
        return recv_msg;
    }
}

