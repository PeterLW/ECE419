package common.cache;
import java.util.*;
import org.apache.log4j.Logger;

public class LFU implements CacheStructure{
    private final static Logger LOGGER = Logger.getLogger(LFU.class);
    private final static HashMap<String, String> vals = new HashMap<String, String>(); // k-v
    private final static HashMap<String, Integer> counts = new HashMap<String, Integer>(); // key - freq
    private final static HashMap<Integer, LinkedHashSet<String>> lists = new HashMap<Integer, LinkedHashSet<String>>(); // freq - keys
    private int capacity;
    private int min = -1;

    public LFU(int capacity) {
        this.capacity = capacity;
    }

    private void initialize(){
        lists.put(1, new LinkedHashSet<String>());
    }

    @Override
    public synchronized String getKV(String key) {
        if (!vals.containsKey(key)) {
        	return null;
        }else{
            updateStructures(key);
            return vals.get(key);
        }
    }

    @Override
    public synchronized boolean putKV(String key, String value) {
        if(capacity <=0)
            return false;

        if (!vals.containsKey(key)){ // not in cache
            if (vals.size() >= capacity) {
                String evitKey = lists.get(min).iterator().next();
                deleteKey(evitKey);
            }
        }
        vals.put(key, value);
        this.updateStructures(key);

        return true;
    }

    private synchronized void updateStructures(String key){
        if (!vals.containsKey(key))
            return; // err...

        Integer count = counts.get(key); // will return null if key doesn't exist
        if (count == null) { // first put
            counts.put(key, 1);
            min = 1;
            lists.get(1).add(key);
        } else { // not first put (already exists in cache)
            counts.put(key, count + 1);
            lists.get(count).remove(key);

            if (count == min && lists.get(count).size() == 0) {
                min++;
            }

            if (!lists.containsKey(count + 1))
                lists.put(count + 1, new LinkedHashSet<String>());
            lists.get(count + 1).add(key);
        }
    }

    @Override
    public synchronized boolean deleteKV(String key){
        if(vals.containsKey(key)) {
            deleteKey(key);
        }
        return true;
    }

    private synchronized void deleteKey(String key){
        vals.remove(key);
        Integer count = counts.get(key);
        if (count == null){
            LOGGER.error("count for key: " + key + " should not be null");
            System.out.println("oh no, it's null!");
        }
        lists.get(count).remove(key);
        counts.remove(key);
    }

    @Override
    public synchronized void clear(){
        vals.clear();
        counts.clear();
        lists.clear();
        initialize();
    }

    @Override
    public boolean inCacheStructure(String key){
        return vals.containsKey(key);
    }

    public int get_cache_size(){
        return capacity;
    }

    @Override
    public void printCacheKeys() {
        Set<String> keys = vals.keySet();
        System.out.println("Printing keys in LFU Cache, capacity: " + capacity);
        for (String key : keys) {
            System.out.print(key + " " + counts.get(key) + ", ");
        }
        System.out.println();
    }



}