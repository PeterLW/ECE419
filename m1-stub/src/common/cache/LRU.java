package common.cache;
import common.cache.Node;
import common.disk.DBManager;
import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

public class LRU {
    private static Logger logger = Logger.getRootLogger();
    int capacity;
    private DBManager database_mgr = null;
    static HashMap<String, Node> map = new HashMap<String, Node>();
    Node head=null;
    Node end=null;

    public LRU(int capacity, DBManager database_mgr) {
        this.capacity = capacity;
        this.database_mgr = database_mgr;
    }

    public synchronized String getKV(String key) {
        if(map.containsKey(key)){
            Node n = map.get(key);
            remove(n);
            setHead(n);
            return n.value;
        }
        else{
            logger.info("key-value pair of "+key+" does not exist in cache");
            String value = database_mgr.getKV(key);
            if(value == null){
                logger.error("key-value pair of "+key+" does not exist in disk");
                return null;
            }
            else{
                //put the <key,pair> to cache now to avoid accessing disk again
                Node created = new Node(key, value);
                if(map.size()>=capacity){
                    map.remove(end.key);
                    remove(end);
                    setHead(created);
                }else{
                    setHead(created);
                }
                map.put(key, created);
                return value;
            }
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
        //here we update the disk to synchronize with the cache. In this way, we no longer need to save the data to be evitcted.
        if(database_mgr.storeKV(key,value) == false){
            logger.error("Error: failed to update <"+key+","+value+"> to disk");
        }
        else {
            logger.info("<" + key + "," + value + "> has been updated to disk");
        }
        return true;
    }

    public void clear(){
        map.clear();
    }

    public boolean in_LRU(String key){
        return map.containsKey(key);
    }

    public int get_cache_size(){
        return capacity;
    }
}