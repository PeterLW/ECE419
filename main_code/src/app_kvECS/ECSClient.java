package app_kvECS;

import java.util.Map;
import java.util.Collection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import ecs.IECSNode;
import ecs.ServerManager;
import ecs.ConfigEntity;
import ecs.ServerNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;


import java.util.*;
import java.io.*;

public class ECSClient implements IECSClient {
    private static final Logger LOGGER = Logger.getLogger(ServerManager.class);
    private static final String PROMPT = "ECSCLIENT> ";
    private static final String CONFIG_FILE_PATH = "ecs.config";

    private final ServerManager serverManager = new ServerManager();
    private final LinkedList<ConfigEntity> entityList = new LinkedList<ConfigEntity>();
    private final BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in)); //Buffered Reader pointed at STDIN

    private boolean stop = false;

    static {
        try {
            new LogSetup("logs/ecsclient.log", Level.DEBUG);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public ECSClient(){}

    @Override
    public boolean start() {
        return serverManager.start();
    }

    @Override
    public boolean stop() {
        return serverManager.stop();
    }

    @Override
    public boolean shutdown() {
        return serverManager.shutdown();
    }

    /**
     * Create a new KVServer with the specified cache size and replacement strategy and add it to the storage service at an arbitrary position.
     * @return  name of new server
     */
    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        LinkedList<IECSNode> list = (LinkedList<IECSNode>) setupNodes(1,cacheStrategy,cacheSize);
        IECSNode n = list.removeFirst();
        /* @Aaron: This is where the ssh script should go
            https://piazza.com/class/jc6l5ut99r35yl?cid=179
        */
        return n;
    }

    /**
     * Randomly choose <numberOfNodes> servers from the available machines and start the KVServer by issuing an SSH call to the respective machine.
     * This call launches the storage server with the specified cache size and replacement strategy. For simplicity, locate the KVServer.jar in the
     * same directory as the ECS. All storage servers are initialized with the metadata and any persisted data, and remain in state stopped.
     * NOTE: Must call setupNodes before the SSH calls to start the servers and must call awaitNodes before returning
     * @return  set of strings containing the names of the nodes
     */
    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> list = setupNodes(count,cacheStrategy,cacheSize);
        for (IECSNode node : list){
            /* @Aaron: This is where the ssh script should go
            https://piazza.com/class/jc6l5ut99r35yl?cid=179
            */

//                serverManager.addNode(cacheStrategy, cacheSize);
            // I need the server name and port to add NOD I CAN'T just add a node with just this...
            // omgosh.
        }
        return list;
    }

    /**
     * Sets up `count` servers with the ECS (in this case Zookeeper)
     * @return  array of strings, containing unique names of servers
     */
    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) { // this function is redundant.
        LinkedList<IECSNode>list = new LinkedList<IECSNode>();
        for(int i = 0; i < count; ++i){
            ServerNode node = new ServerNode(entityList.removeFirst(), cacheSize, cacheStrategy);
            list.add(node);
            try {
                serverManager.addKVServer(node, cacheStrategy,cacheSize);
            } catch (InterruptedException | KeeperException e) {
                LOGGER.error("Trying to add KVServer " + node.getNodeName() + " to Zookeeper, Failed due to exception ", e);
                System.out.println("Trying to add KVServer " + node.getNodeName() + " to Zookeeper, Failed due to exception ");
                e.printStackTrace();
            }
        }
        return list;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        for (String name : nodeNames) {
            try {
                serverManager.removeKVServer(name);
            }catch (KeeperException | InterruptedException e){
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        /* this function is only for their autotester, so I'm going to cast everything to
            a HashMap<String,IECSNode>
         */
        HashMap<String,IECSNode> result = new HashMap<String,IECSNode>();
        HashMap<String, ServerNode> s = serverManager.getServerMap();

        Set<String> keys = s.keySet();
        for (String key : keys){
            result.put(key,s.get(key));
        }
        return result;
    }

    @Override
    public IECSNode getNodeByKey(String key) {
        ServerNode n = serverManager.getServerForKey(key);
        if (n == null){
            LOGGER.error("Attempting to get Server responsible for key: " + key + ", returned null");
            return null;
        }
        return n;
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

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("shutdown")) {
            stop = true;
            System.out.println(PROMPT + "All servers are shutdown.");
        } else if (tokens[0].equals("start")){
            this.start();
        } else  if (tokens[0].equals("stop")) {
            this.stop();
        } else if(tokens[0].equals("addNode")){
            if(tokens.length != 3) {
                printError("Invalid number of arguments");
            }else {

                int cacheSize = Integer.parseInt(tokens[1]);
                String cacheStrategy = tokens[2];
                if (addNode(cacheStrategy, cacheSize) == null) {
                    printError("Error occurred in adding a server node");
                }
                else{
                    System.out.println("new server node added to the ECS");
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
                    printError("Failed in adding "+tokens[1]+" server nodes");
                }
                else{
                    System.out.println(tokens[1] + " nodes added to the ECS");
                }
            }
        } else if(tokens[0].equals("removeNode")) {
            int num_args = tokens.length;
            if(num_args < 2) {
                printError("Invalid number of arguments");
            }
            else{
                    ArrayList<String> nodes = new ArrayList<String>();
                    for(int i = 1; i < num_args; i++){
                        nodes.add(tokens[i]);
                    }
                    if(removeNodes(nodes) == false){
                        printError("failed to remove " + Integer.toString(num_args - 1) +" nodes");
                    }
                    else{
                        System.out.println("node deletion succeed");
                    }
            }
        } else if(tokens[0].equals("help")) {
            help();
        }
        else{
            printError("Unknown command");
            help();
        }
    }

    private void help() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECSCLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");

        sb.append(PROMPT).append("addNodes <numberOfNodes> <cacheSize> <replacementStrategy>");
        sb.append("\t launch server nodes\n");

        sb.append(PROMPT).append("start");
        sb.append("\t\t starts the storage services on all participating storage servers \n");

        sb.append(PROMPT).append("stop");
        sb.append("\t\t stops the storage services on all participating storage servers \n");

        sb.append(PROMPT).append("shutDown");
        sb.append("\t\t Stops all server instances and exits the remote processes. \n");

        sb.append(PROMPT).append("addNode <cacheSize> <replacementStrategy>");
        sb.append("\t\t Add a new server to the storage service at an arbitrary position .\n");
        sb.append(PROMPT).append("removeNode <server_index> ");
        sb.append("\t\t Remove a server from the storage service at an arbitrary position. \n");
    }


    public void run(){
        // start parseConfigFile with path to file
       // parseConfigFile(CONFIG_FILE_PATH);
        while(!stop) {
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

        ECSClient client = new ECSClient();
        client.run();
        System.exit(1);

    }
}
