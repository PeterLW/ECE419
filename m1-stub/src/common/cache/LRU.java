package common.cache;
import common.cache.Node;
import common.disk.DBManager;

import java.io.IOException;
import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

public class LRU implements CacheStructure{
    private static Logger logger = Logger.getLogger(LRU.class);
    int capacity;
    static HashMap<String, Node> map = new HashMap<String, Node>();
    Node head=null;
    Node end=null;

    public LRU(int capacity, DBManager database_mgr) {
        this.capacity = capacity;
    }

    @Override
    public void printCacheKeys(){
        Set<String> keys = map.keySet();
        System.out.println("Printing keys in LRU Cache, capacity: " + capacity);
        for (String key : keys) {
            System.out.print(key + ", ");
        }
        System.out.println();
    }

    @Override
    public synchronized String getKV(String key) {
        if(map.containsKey(key)){
            Node n = map.get(key);
            remove(n);
            setHead(n);
            return n.value;
        }else{
        	return null;
        }
    }

    private void remove(Node n){
        if(n.pre!=null){
            n.pre.next = n.next;
        }else{
            head = n.next;
        }
        if(n.next!=null){
            n.next.pre = n.pre;
        }else{
            end = n.pre;
        }
    }

    private void setHead(Node n){
        n.next = head;
        n.pre = null;

        if(head!=null)
            head.pre = n;
        head = n;
        if(end ==null)
            end = head;
    }

    @Override
    public synchronized boolean deleteKV(String key){

        if(map.containsKey(key)) {
            Node n = map.get(key);
            remove(n);
            map.remove(key);
        }
        return true;
    }

    @Override
    public synchronized boolean putKV(String key, String value) {
        if(map.containsKey(key)){
            Node old = map.get(key);
            old.value = value;
            remove(old);
            setHead(old);
        }else{
            Node created = new Node(key, value);
            if(map.size()>=capacity){
                    map.remove(end.key);
                    remove(end);
                    setHead(created);
            }else{
                setHead(created);
            }

            map.put(key, created);
        }
        return true;
    }

    @Override
    public void clear(){
        map.clear();
    }

    @Override
    public boolean inCacheStructure(String key){
        return map.containsKey(key);
    }

    public int get_cache_size(){
        return capacity;
    }
}