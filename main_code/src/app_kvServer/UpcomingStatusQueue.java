package app_kvServer;


import java.util.*;

public class UpcomingStatusQueue {
    private boolean isReady = false;
    private LinkedList<ServerStatus> upcomingStatus = new LinkedList<ServerStatus>();
    /*check concurrentLinkedQueue & LinkedBlockingDeque*/

    //Peter: want to remove and return first element? Use removeFirst()
    public synchronized ServerStatus popQueue(){
        return upcomingStatus.removeFirst();
    }

    public synchronized void addQueue(ServerStatus statusNode){
        upcomingStatus.add(statusNode);
    }

    /* double check */
    public synchronized ServerStatus peakQueue(){
        return upcomingStatus.peek();
    }
}
