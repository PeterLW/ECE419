package common.zookeeper;

import java.util.*;

/**
 * This class is used as a 'message' of sorts between the ECSClient and the ZookeeperHeartbeatWatcher
 * It keeps track of servers that ECS client is receiving heartbeats from
 *
 * There is also a status value, so that the ECSClient won't take command line inputs after a
 *  'dead' node is discovered, and the removeNode procedure is called for dead node
 */
public class HeartbeatTracker {
    private HashMap<String, Integer> countsPerServer = new HashMap<String, Integer>();
    private TreeMap<Integer, LinkedHashSet<String>> serversPerCount = new TreeMap<Integer, LinkedHashSet<String>>();
    private int min = 0, max = 0;

    public HeartbeatTracker(){
        serversPerCount.put(new Integer(0),new LinkedHashSet<String>());
    }

    public synchronized boolean addServer(String serverName){
        if (countsPerServer.containsKey(serverName)){
            return false;
        }

        if ((max-min) <= 1) {
            countsPerServer.put(serverName, min);
            updateCountsList(-1, min,serverName);
        } else if ((max-min) <=2){
            countsPerServer.put(serverName, min+1); // doesn't accidently become 'dead'
            updateCountsList(-1, min+1,serverName);
        } else {
            countsPerServer.put(serverName, min+2); // doesn't accidently become 'dead'
            updateCountsList(-1, min+2,serverName);

        }
        System.out.println(serverName + " added ");


        return true;
    }

    public synchronized boolean removeServer(String serverName) {
        if (!countsPerServer.containsKey(serverName)) {
            return false;
        }

        Integer count = countsPerServer.get(serverName);
        serversPerCount.get(count).remove(serverName);
        if (serversPerCount.get(count).isEmpty()){
            serversPerCount.remove(count);
            if (count == max){
                max = serversPerCount.lastKey();
            }
            if (count == min){
                min = serversPerCount.firstKey();
            }
        }

        countsPerServer.remove(serverName);
        return true;
    }

    public synchronized boolean appendCount(String serverName){
        if (!countsPerServer.containsKey(serverName)){
            return false;
        }

        Integer count = countsPerServer.get(serverName);
        Integer newCount = count + 1;
        countsPerServer.put(serverName, newCount);
        updateCountsList(count,newCount, serverName);
        System.out.println(serverName + " count: " + count + " new count: " + newCount + " min: " + min + " max: " + max);
        return true;
    }

    public synchronized String getDead(){
        if ((max - min) <= 5){
            return null;
        }

        LinkedHashSet<String> list = serversPerCount.get(min);
        if (list.size() > 1){
            System.out.println("size of min is: " + list.size());
        }

        String serverName = list.iterator().next();
        System.out.println(serverName + " is detected to be dead. ");
        return serverName;
    }

    private void updateCountsList(Integer oldCount, Integer newCount, String serverName){
        if (!serversPerCount.containsKey(newCount)){
            serversPerCount.put(newCount,new LinkedHashSet<String>());
        }

        serversPerCount.get(newCount).add(serverName);
//        System.out.println("here " + oldCount + " " + newCount);

        // no old
        if (oldCount < 0){
//            System.out.println(oldCount);
            return;
        }

        // old
        serversPerCount.get(oldCount).remove(serverName);
        if (serversPerCount.get(oldCount).isEmpty()){
            serversPerCount.remove(oldCount);
            if (min == oldCount){
                min++; // can only go up by 1
                if (max < min){
                    max = min;
                }
            }
        }

        if (max < newCount){
            max = newCount;
        }

    }

    public Integer getMin(){
        return min;
    }

}
