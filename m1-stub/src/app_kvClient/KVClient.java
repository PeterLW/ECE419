package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import common.messages.Message;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;
import common.messages.Message;
import client.KVCommInterface;
import client.KVStore;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getLogger(KVClient.class);
    private static final String PROMPT = "KVCLIENT> ";
    private BufferedReader stdin;

    private KVStore kvStore;
    private boolean stop = false;


    @Override
    public void newConnection(String hostname, int port) {
        // TODO Auto-generated method stub
        try {
            kvStore = new KVStore(hostname, port); // API we have to implement
            kvStore.connect();
        } catch (Exception ioe) {
            logger.error("Unable to connect!");
        }
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return kvStore;
    }


    public void quit(){
        kvStore.disconnect();
        stop = true;
        return;
    }

    public void logLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
        } else {
            printError(LogSetup.UNKNOWN_LEVEL.toString());
        }
    }

    public void help() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KVCLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");

        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t put a key-value pair into the storage server \n");

        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t retrieve value for the given key from the storage server \n");

        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel <level>");
        sb.append("\t\t\t set logger to the specified log level \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t Tears down the active connection to the server and exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private void handleCommand(String cmdLine) { // CLI Class probably should just be modifying this is enough .__.
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            quit();
            System.out.println(PROMPT + "Application exit!");
        } else if (tokens[0].equals("connect")){
            if(tokens.length == 3) {
                String serverAddress = tokens[1];
                int serverPort = Integer.parseInt(tokens[2]);
                this.newConnection(serverAddress, serverPort);
            } else {
                printError("Invalid number of parameters!");
            }
        } else  if (tokens[0].equals("put")) {
            if(tokens.length >= 2) {
                if(kvStore != null && kvStore.isRunning()){
                    StringBuilder value = new StringBuilder();
                    value.setLength(0);
                    for(int i = 2; i < tokens.length; i++) {
                        value.append(tokens[i]);
                        if (i != tokens.length - 1 ) {
                            value.append(" ");
                        }
                    }
                    try {
                        Message msg = kvStore.put(tokens[1], value.toString()); // blocking call
                        if(msg == null){
                            printError("Communication for PUT failed");
                        }
                    }catch(Exception e){
                        printError("Put fail!");
                        logger.warn("Put fail!", e);
                    }
                } else {
                    printError("Not connected!");
                }
            } else {
                printError("No key-value pair passed!");
            }

        } else if(tokens[0].equals("get")){
            if(tokens.length == 2) {
                if(kvStore != null && kvStore.isRunning()){
                    try {
                        Message msg = kvStore.get(tokens[1]); // blocking call
                        if(msg == null){
                            printError("Communication for GET failed");
                        }
                    }catch(Exception e){
                        printError("Get fail!");
                        logger.warn("Get fail!", e);
                    }
                } else {
                    printError("Not connected!");
                }
            } else {
                if(tokens.length < 2) printError("No key passed!");
                if(tokens.length > 2) printError("Too many arguments");
            }
        }else if(tokens[0].equals("disconnect")) {
            kvStore.disconnect();

        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                logLevel(tokens[1]);
            } else {
                printError("Invalid number of parameters!");
            }
        } else if(tokens[0].equals("help")) {
            help();
        }
        else if(tokens[0].equals("clear")){
            if(tokens[1].equals("cache")) {
                kvStore.clear("cache");
            }
            else if(tokens[1].equals("disk")) {
                kvStore.clear("disk");
            }
            else {
                printError("Invalid storage device to be cleared");
            }
        }
        else
        } {
            printError("Unknown command");
            help();
        }
    }

    public void run(){
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in)); //Buffered Reader pointed at STDIN
            System.out.print(PROMPT);
            try {
                String cmdLine = stdin.readLine(); // reads input after prompt
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    public static void main(String[] args){
        try {
            new LogSetup("logs/application.log", Level.INFO); // debug - setting log to info level
            KVClient client = new KVClient();
            client.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

}
