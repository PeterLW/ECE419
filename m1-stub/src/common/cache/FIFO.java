package common.cache;

import java.util.*;
import common.disk.DBManager;
import org.apache.log4j.Logger;


public class FIFO implements CacheStructure{
    private int size;
    private static LinkedHashMap<String,String> fifo;
    private final static Logger logger = Logger.getLogger(FIFO.class);
    private static DBManager database_mgr;

    public FIFO(int size, DBManager database_mgr) {
        this.size = size;
        this.fifo = new LinkedHashMap<String,String>();
        this.database_mgr = database_mgr;
    }

    @Override
    public synchronized boolean putKV(String key, String value){
        if(fifo.size() >= size){
            fifo.remove(fifo.entrySet().iterator().next().getKey());
        }
        fifo.put(key,value);
        return true;
    }

    @Override
    public synchronized String getKV(String key){
        if (fifo.containsKey(key)){
            return fifo.get(key);
        }
        else{
        	return null;
        }
    }

    @Override
    public synchronized boolean deleteKV(String key){

        if (fifo.containsKey(key)) {
            fifo.remove(key);
            return database_mgr.deleteKV(key);
        }

        return database_mgr.deleteKV(key);

    }

    @Override
    public void clear(){
        fifo.clear();
    }

    @Override
    public boolean inCacheStructure(String key){
        return fifo.containsKey(key);
    }

    @Override
    public void printCacheKeys() {

    }

    public int get_cache_size(){
        return size;
    }
}