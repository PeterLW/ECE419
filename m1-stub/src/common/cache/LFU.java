package common.cache;
import java.util.*;
import common.disk.DBManager;
import org.apache.log4j.Logger;

public class LFU implements CacheStructure{
    private final static Logger LOGGER = Logger.getLogger(LFU.class);
    private static HashMap<String, String> vals;
    private static HashMap<String, Integer> counts;
    private static HashMap<Integer, LinkedHashSet<String>> lists;
    private int capacity;
    private int min = -1;

    public LFU(int capacity, DBManager database_mgr) {
        this.capacity = capacity;
        vals = new HashMap<>();
        counts = new HashMap<>();
        lists = new HashMap<>();
        lists.put(1, new LinkedHashSet<String>());
    }


    @Override
    public void printCacheKeys() {
        Set<String> keys = vals.keySet();
        System.out.println("Printing keys in LRU Cache, capacity: " + capacity);
        for (String key : keys) {
            System.out.print(key + ", ");
        }
        System.out.println();
    }


    @Override
    public synchronized String getKV(String key) {

        if (!vals.containsKey(key)) {
        	return null;
        }else{
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
    }

    public synchronized void keyDelete(String key){

        vals.remove(key);
        int count = counts.get(key);
        lists.get(count).remove(key);
        counts.remove(key);
        return;
    }

    @Override
    public synchronized boolean deleteKV(String key){

        if(vals.containsKey(key)) {
            keyDelete(key);
        }
        return true;
    }

    @Override
    public synchronized boolean putKV(String key, String value) {
        if(capacity <=0)
            return false;
        if(vals.containsKey(key)) {
            vals.put(key, value);
            getKV(key);
        }

        if (vals.size() >= capacity) {
            String evit = lists.get(min).iterator().next();
            lists.get(min).remove(evit);
            vals.remove(evit);
        }
        
        vals.put(key, value);
        counts.put(key, 1);
        min = 1;
        lists.get(1).add(key);
       
        return true;
    }

    @Override
    public void clear(){
        vals.clear();
        counts.clear();
        lists.clear();
    }

    @Override
    public boolean inCacheStructure(String key){
        return vals.containsKey(key);
    }

    public int get_cache_size(){
        return capacity;
    }


}