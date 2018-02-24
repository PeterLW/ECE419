package common.ecs;


import com.sun.security.ntlm.Server;

import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

public class ServerManager {
    // hash values -> serverNode
    private TreeMap<String,ServerNode> tree = new TreeMap<String,ServerNode>();
    // ip:port -> serverNode
    private HashMap<String,ServerNode> hash = new HashMap<String,ServerNode>();

    public ServerManager(){
        
    }

}
