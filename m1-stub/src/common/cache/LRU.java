package common.cache;
import common.disk.DBManager;

import java.util.*;

import org.apache.log4j.Logger;

public class LRU implements CacheStructure{
    private static Logger LOGGER = Logger.getLogger(LRU.class);
    private int capacity;
    private static HashMap<String, LRUNode> map = new HashMap<String, LRUNode>();
    private static LRUNode head=null;
    private static LRUNode end=null;

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
            LRUNode n = map.get(key);
            remove(n);
            setHead(n);
            return n.value;
        }else{
        	return null;
        }
    }

    private void remove(LRUNode n){
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

    private void setHead(LRUNode n){
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
            LRUNode n = map.get(key);
            remove(n);
            map.remove(key);
        }
        return true;
    }

    @Override
    public synchronized boolean putKV(String key, String value) {
        if(map.containsKey(key)){
            LRUNode old = map.get(key);
            old.value = value;
            remove(old);
            setHead(old);
        }else{
            LRUNode created = new LRUNode(key, value);
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