package app_kvECS;

import java.util.Map;
import java.util.Collection;

import ecs.IECSNode;
import ecs.ServerManager;
import ecs.ConfigEntity;
import ecs.ServerNode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;


import java.util.*;
import java.io.*;

public class ECSClient implements IECSClient {

    private static final Logger LOGGER = Logger.getLogger(ServerManager.class);
    private final ServerManager serverManager = new ServerManager();
    private LinkedList<ConfigEntity> entityList= new LinkedList<ConfigEntity>();
    private static final String PROMPT = "ECSCLIENT> ";
    private static final String CONFIG_FILE_PATH = "ecs.config";
    private boolean stop;


    public ECSClient(){

        this.stop = false;
    }

    @Override
    public boolean start() {

        // start parseConfigFile with path to file
        parseConfigFile(CONFIG_FILE_PATH);
        return serverManager.start();
    }

    @Override
    public boolean stop() {

        return serverManager.stop();
    }

    @Override
    public boolean shutdown() {
        // TODO
        return serverManager.shutdown();
    }

    //@return  name of new server
    @Override
    public ServerNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        if(entityList.isEmpty()){
            printError("no more nodes are available");
            return null;
        }
        else{
            ServerNode node = new ServerNode(entityList.removeFirst(),cacheSize,cacheStrategy);
            if(serverManager.addNode(node) == false){
                printError("Error: failed to error server node to server manager");
            }
            return node;
        }
    }

    //@return  set of strings containing the names of the nodes
    @Override
    public Collection<ServerNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        for (int i =0; i<count; i++){

            ServerNode n = new ServerNode(entityList.removeFirst(), cacheSize, cacheStrategy);
            try {
                serverManager.addNode(n);
            } catch (KeeperException | InterruptedException e) {
                LOGGER.error("Error attempting to add node #" + count + " in addNodes. Node name: " + n.getNodeName());
            }
        }
        //need to change here through ServerManager API.
        return null;
    }

    @Override
    public Collection<ServerNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {

        for (Iterator<String> iterator = nodeNames.iterator(); iterator.hasNext();) {

            serverManager.removeNode(iterator.next());
        }

    }

    @Override
    public Map<String, ServerNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    /**
     * parse the ecs.config file to get a list of IPs
     * @return a string array containing info regarding one machine
     */
    private void parseConfigFile(String filePath){
        try {
            File file = new File(filePath);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            StringBuffer stringBuffer = new StringBuffer();

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuffer.append(line);
                stringBuffer.append(",");
            }
            fileReader.close();
            String machine_list = stringBuffer.toString();
            String[] splitArray = machine_list.split(",");

            int length = splitArray.length;
            for(int i = 0; i < length; i++){
                String[] entry = splitArray[i].split("\\s+");
                ConfigEntity node = new ConfigEntity(entry[0],entry[1],Integer.parseInt(entry[2]));
                entityList.add(node);
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    private void handleCommand(String cmdLine) { // CLI Class probably should just be modifying this is enough .__.
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("shutdown")) {
            stop = true;
            System.out.println(PROMPT + "All servers are shutdown.");
        } else if (tokens[0].equals("start")){
            start();
        } else  if (tokens[0].equals("stop")) {
            stop();
        } else if(tokens[0].equals("addNode")){
            if(tokens.length != 3) {
                printError("Invalid number of arguments");
            }else {

                int cacheSize = Integer.parseInt(tokens[1]);
                String cacheStrategy = tokens[2];
                if (addNode(cacheStrategy, cacheSize) == null) {
                    printError("Error occurred in adding a server node");
                }
            }
        }else if(tokens[0].equals("addNodes")) {
            if(tokens.length != 4){
                printError("Invalid number of arguments");
            }
            else{
                int count = Integer.parseInt(tokens[1]);
                int cacheSize = Integer.parseInt(tokens[2]);
                String cacheStrategy = tokens[3];
                if(addNodes(count,cacheStrategy,cacheSize) == null){
                    printError("Error occurred in adding "+tokens[1]+" server nodes");
                }
            }
        } else if(tokens[0].equals("removeNode")) {
            if(tokens.length != 2) {
                printError("Invalid number of arguments");
            }
            else{

            }
        } else if(tokens[0].equals("help")) {
            help();
        }
        else if(tokens[0].length() == 0){
            //do nothing
        }
        else{
            System.out.println("enter key = "+tokens[0]);
            printError("Unknown command");
            help();
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

    public static void main(String[] args) {
        // TODO
        ECSClient client = new ECSClient();
        client.run();
        System.exit(1);
        //parse the config file

    }
}
