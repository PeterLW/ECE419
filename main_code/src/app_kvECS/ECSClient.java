package app_kvECS;

import java.util.Collection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import ecs.IECSNode;
import ecs.ServerManager;
import ecs.ServerNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.util.*;

public class ECSClient implements IECSClient {

    public enum SYSTEM_STATE{
        INIT,
        RUNNING
    }

    private static final Logger LOGGER = Logger.getLogger(ServerManager.class);
    private static final String PROMPT = "ECSCLIENT> ";
    private static final int timeout = 5000;

    //Buffered Reader pointed at STDIN
    private final BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    private final ServerManager serverManager = new ServerManager();

    private boolean stop = false;
    private SYSTEM_STATE system_state = SYSTEM_STATE.INIT;

    static {
        try {
            new LogSetup("logs/ecsclient.log", Level.DEBUG);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public ECSClient(){system_state = SYSTEM_STATE.INIT;}

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

        Collection<IECSNode>list = setupNodes(1,cacheStrategy,cacheSize);
        boolean flag;
        try {
            flag = awaitNodes(1, timeout);
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
       return (flag == true) ? list.iterator().next():null;

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
        Collection<IECSNode>list = setupNodes(count,cacheStrategy,cacheSize);
        boolean flag;
        try {
            flag = awaitNodes(count, timeout);
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        return (flag ? list : null); // if(flag == true) is same as if(flag)
    }

    /**
     * Sets up `count` servers with the ECS (in this case Zookeeper)
     * @return  array of strings, containing unique names of servers
     */
    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) { // this function is redundant.
        LinkedList<IECSNode>list = new LinkedList<IECSNode>();
        try {
            if(system_state == SYSTEM_STATE.INIT) {
                for (int i = 0; i < count; ++i) {
                    ServerNode node = serverManager.addNode(cacheSize, cacheStrategy);
                    list.add(node);
                }
                serverManager.updateMetaDataZNode();
                for(int i = 0; i < count; ++i){
                    serverManager.remoteLaunchServer(list.get(i).getNodePort());
                }
                system_state = SYSTEM_STATE.RUNNING;
            }
            else{
                for(int i = 0; i < count; ++i){
                    ServerNode node = serverManager.addNode(cacheSize, cacheStrategy);
                    list.add(node);
                    serverManager.updateMetaDataZNode();
                    serverManager.remoteLaunchServer(list.get(i).getNodePort());
                }
            }
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error("Failed to update metadata");
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {

        long endTimeMillis = System.currentTimeMillis() + timeout;
        while (true) {
            // method logic
            if (serverManager.getNumOfServerConnected() >= count)
                break;
            if (System.currentTimeMillis() > endTimeMillis) {
                // do some clean-up
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        for (String name : nodeNames) {
            try {
                if(! serverManager.removeNode(name)){
                    return false;
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    public HashMap<String, IECSNode> getNodes() {
        return serverManager.getServerMap();
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return serverManager.getServerName(Key);
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
                    if(!removeNodes(nodes)){
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
        System.out.println(sb);

    }

    public void run(){
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
