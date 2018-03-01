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



    @Override
    public boolean start() {
        // TODO
        // get command line argument
        // start parseConfigFile with path to file
        parseConfigFile("ecs.config");
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        for (int i =0; i<count; i++){
            ServerNode n = new ServerNode(entityList.removeFirst());
            try {
                serverManager.addNode(n);
            } catch (KeeperException | InterruptedException e) {
                LOGGER.error("Error attempting to add node #" + count + " in addNodes. Node name: " + n.getNodeName());
            }
        }
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
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
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
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

    public static void main(String[] args) {
        // TODO

        //parse the config file

    }
}
