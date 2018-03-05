package app_kvServer;
import java.util.*;

public class UpcomingServerStatus {

    private PriorityQueue<ServerStatus> statusQueue = new PriorityQueue<ServerStatus>();

    public UpcomingServerStatus(){
        //do nothing...
    }

    public ServerStatus popStatusNode(){

        return statusQueue.poll();
    }
    public void addStatusNode(ServerStatus statusNode){

        statusQueue.add(statusNode);
    }
}
