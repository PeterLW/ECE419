package ecs;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

public class ServerManager {
    // hash values (start) -> serverNode
    private TreeMap<String,ServerNode> tree = new TreeMap<String,ServerNode>();
    // ip:port -> serverNode
    private HashMap<String,ServerNode> hashMap = new HashMap<String,ServerNode>();

    public ServerManager(){

    }

    public boolean addNode(ServerNode n){
        String id = n.getNodeId(); // ip:port
        if (hashMap.containsKey(id)) {
            return false;
        }
        hashMap.put(id,n);

        return true;
    }

    /**
     * @return number of nodes successfully added
     */
    public int addNodes(List<ServerNode> nodes){
        return 0;
    }

    public boolean removeNode(String id) {
        ServerNode n = hashMap.get(id);
        if (n == null){
            return false;
        }
        String hash = n.getNodeHashRange()[0];

        if(!tree.containsKey(hash)){
            hashMap.remove(id);
            throw new RuntimeException(id + " was found in hashMap but not in tree");
        }

        hashMap.remove(id);
        tree.remove(hash);
        return true;
    }

    /**
     * @return number of nodes successfully removed
     */
    public int removeNodes(List<String> ids){
        return 0;
    }

}
