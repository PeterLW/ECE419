package app_kvServer;
import java.util.*;

public class ServerStatusQueue {

    private PriorityQueue<KVServerStatus> statusQueue = new PriorityQueue<KVServerStatus>();

    public ServerStatusQueue(){
        //do nothing...
    }

    public KVServerStatus popStatusNode(){

        return statusQueue.poll();
    }
    public void addStatusNode(KVServerStatus statusNode){

        statusQueue.add(statusNode);
    }
}
