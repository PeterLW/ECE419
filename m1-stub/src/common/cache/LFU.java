package common.cache;
import java.util.*;
import common.disk.DBManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

public class LFU{
    private static Logger logger = Logger.getRootLogger();
    private static HashMap<String, String> vals;
    private static HashMap<String, Integer> counts;
    private static HashMap<Integer, LinkedHashSet<String>> lists;
    private int cap;
    private int min = -1;
    private DBManager database_mgr=null;

    public LFU(int capacity, DBManager database_mgr) {
        this.cap = capacity;
        this.database_mgr = database_mgr;
        this.vals = new HashMap<>();
        this.counts = new HashMap<>();
        this.lists = new HashMap<>();
        this.lists.put(1, new LinkedHashSet<String>());
    }


    public synchronized String getKV(String key) {

        if (vals.containsKey(key) == true) {

            int count = counts.get(key);
            counts.put(key, count + 1);
            lists.get(count).remove(key);
            if (count == min && lists.get(count).size() == 0)
                min++;
            if (!lists.containsKey(count + 1))
                lists.put(count + 1, new LinkedHashSet<String>());
            lists.get(count + 1).add(key);
            return vals.get(key);
        }
        else {
            logger.info("key-value pair of "+key+" does not exist in cache");
            String value = database_mgr.getKV(key);
            if(value == null){
                logger.error("key-value pair of "+key+" does not exist in disk");
                return null;
            }
            else {
                //put the <key,pair> to cache now to avoid accessing disk again
                if (vals.size() >= cap) {
                    String evit = lists.get(min).iterator().next();
                    lists.get(min).remove(evit);
                    vals.remove(evit);
                }
                vals.put(key, value);
                counts.put(key, 1);
                min = 1;
                lists.get(1).add(key);
                return value;
            }
        }
    }

    public synchronized boolean putKV(String key, String value) {
        if(cap<=0)
            return false;
        if(vals.containsKey(key)) {
            vals.put(key, value);
            getKV(key);
            //update the disk
            if(database_mgr.storeKV(key, value) == false){
                logger.error("Error: < "+key+","+value+"> failed to write to disk");
            }
        }
        else {
            if (vals.size() >= cap) {
                String evit = lists.get(min).iterator().next();
                lists.get(min).remove(evit);
                vals.remove(evit);
            }
            vals.put(key, value);
            counts.put(key, 1);
            min = 1;
            lists.get(1).add(key);
        }
        //update the disk
        if(database_mgr.storeKV(key,value) == false){
            logger.error("Error: failed to update <"+key+","+value+"> to disk");
        }
        else {
            logger.info("<" + key + "," + value + "> has been updated to disk");
        }
        return true;
    }

    public void clear(){
        vals.clear();
        counts.clear();
        lists.clear();
    }

    public boolean in_LFU(String key){
        return vals.containsKey(key);
    }

    public int get_cache_size(){
        return cap;
    }


}