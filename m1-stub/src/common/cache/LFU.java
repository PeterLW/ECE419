package common.cache;
import java.util.*;

public class LFU{
    private static HashMap<String, String> vals;
    private static HashMap<String, Integer> counts;
    private static HashMap<Integer, LinkedHashSet<String>> lists;
    private int cap;
    private int min = -1;

    public LFU(int capacity) {
        cap = capacity;
        vals = new HashMap<>();
        counts = new HashMap<>();
        lists = new HashMap<>();
        lists.put(1, new LinkedHashSet<String>());
    }

    public String getKV(String key) {
        if(!vals.containsKey(key))
            return null;
        int count = counts.get(key);
        counts.put(key, count+1);
        lists.get(count).remove(key);
        if(count==min && lists.get(count).size()==0)
            min++;
        if(!lists.containsKey(count+1))
            lists.put(count+1, new LinkedHashSet<String>());
        lists.get(count+1).add(key);
        return vals.get(key);
    }

    public void putKV(String key, String value) {
        if(cap<=0)
            return;
        if(vals.containsKey(key)) {
            vals.put(key, value);
            getKV(key);
            return;
        }
        if(vals.size() >= cap) {
            String evit = lists.get(min).iterator().next();
            lists.get(min).remove(evit);
            vals.remove(evit);
        }
        vals.put(key, value);
        counts.put(key, 1);
        min = 1;
        lists.get(1).add(key);
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