package common.cache;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import common.disk.DBManager;

public class CacheManager {

    private static final Logger logger = Logger.getLogger(CacheManager.class);
    private static int cache_size;
    private static String strategy;
    private static LRU lru_cache = null;
    private static LFU lfu_cache = null;
    private static FIFO fifo_cache = null;
    private static DBManager data_base_mgr = null;

    static {
        try {
            new logger.LogSetup("logs/storage.log", Level.INFO);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public CacheManager(int size, String cache_strategy, DBManager data_base_mgr) {
        cache_size = size;
        strategy = cache_strategy;
        data_base_mgr = data_base_mgr;

        if(cache_strategy.equals("FIFO"))
            fifo_cache = new FIFO(size,data_base_mgr);
        else if(cache_strategy.equals("LFU"))
            lfu_cache = new LFU(size,data_base_mgr);
        else if(cache_strategy.equals("LRU"))
            lru_cache = new LRU(size,data_base_mgr);
        else
            logger.error("Error: Invalid cache strategy !");
    }

    public boolean putKV(String key, String value){

        if(strategy.equals("FIFO"))
            return fifo_cache.putKV(key,value);
        else if(strategy.equals("LFU"))
            return lfu_cache.putKV(key,value);
       else
            return lru_cache.putKV(key,value);
    }

    public String getKV(String key){

        if(strategy.equals("FIFO"))
            return fifo_cache.getKV(key);
        else if(strategy.equals("LFU"))
            return lfu_cache.getKV(key);
        else
            return lru_cache.getKV(key);
    }

    public void clear(){

        if(strategy.equals("FIFO"))
            fifo_cache.clear();
        else if(strategy.equals("LFU"))
            lfu_cache.clear();
        else
            lru_cache.clear();
    }

    public boolean inCache(String key){
        if(strategy.equals("FIFO"))
            return fifo_cache.in_fifo(key);
        else if(strategy.equals("LFU"))
            return lfu_cache.in_LFU(key);
        else
            return lru_cache.in_LRU(key);
    }

    public boolean doesKeyExist(String key) {
        if (inCache(key) || data_base_mgr.isExists(key)){
            return true;
        }
        return false;
    }

    public boolean cache_delete(String key){

        if(strategy.equals("FIFO"))
            return fifo_cache.delete(key);
        else if(strategy.equals("LFU"))
            return lfu_cache.delete(key);
        else
            return lru_cache.delete(key);
    }

    public int get_cache_size(){
        return cache_size;
    }


}