package app_kvECS;

import java.util.Map;
import java.util.Collection;

import ecs.IECSNode;
import ecs.ServerManager;
import ecs.ConfigEntity;
import java.util.*;

public class ECSClient implements IECSClient {
    private ServerManager serverManager;

    @Override
    public boolean start() {
        // TODO
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
        // TODO
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
    private ConfigEntity[]parseConfigFile(){

        try {
            File file = new File("ecs.config");
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            StringBuffer stringBuffer = new StringBuffer();
            ArrayList<ConfigEntity> entityList=new ArrayList<String>();//Creating arraylist

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
                ConfigEntity node = new ConfigEntity(entry[0],entry[1],entry[2]);
                entityList.add(node);
            }
            bufferedReader.close();
            return entityList;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // TODO

        //parse the config file

    }
}
