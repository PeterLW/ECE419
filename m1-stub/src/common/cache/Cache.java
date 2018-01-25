package common.cache;
import java.io.IOException;
import java.util.*;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import common.cache.FIFO;
import common.cache.LFU;
import common.cache.LRU;
import common.disk.DBManager;

public class Cache{

    private static final Logger logger = Logger.getLogger(Cache.class);
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


    public Cache(int size, String cache_strategy, DBManager data_base_mgr) {
        this.cache_size = size;
        this.strategy = cache_strategy;
        this.data_base_mgr = data_base_mgr;

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

        if(assertKey(key)){
            if (strategy.equals("FIFO"))
                return fifo_cache.putKV(key, value);
            else if (strategy.equals("LFU"))
                return lfu_cache.putKV(key, value);
            else
                return lru_cache.putKV(key, value);
        }
        else{
            logger.error("Error: Invalid key !");
            return false;
        }
    }

    public String getKV(String key){

        if(assertKey(key)){
            if (strategy.equals("FIFO"))
                return fifo_cache.getKV(key);
            else if (strategy.equals("LFU"))
                return lfu_cache.getKV(key);
            else
                return lru_cache.getKV(key);
        }
        else{
            logger.error("Error: Invalid key !");
            return null;
        }
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

        if(assertKey(key)){
            if (strategy.equals("FIFO"))
                return fifo_cache.inCacheStructure(key);
            else if (strategy.equals("LFU"))
                return lfu_cache.inCacheStructure(key);
            else
                return lru_cache.inCacheStructure(key);
        }
        else{
            logger.error("Error: Invalid key !");
            return false;
        }
    }

    public boolean cacheDelete(String key){

        if(assertKey(key)) {
            if (strategy.equals("FIFO"))
                return fifo_cache.deleteKV(key);
            else if (strategy.equals("LFU"))
                return lfu_cache.deleteKV(key);
            else
                return lru_cache.deleteKV(key);
        }
        else{
            logger.error("Error: Invalid key !");
            return false;
        }
    }

    public boolean assertKey(String key){
        if(key == null || key.isEmpty() == true) {
            return false;
        }
        return true;
    }

    public int get_cache_size(){
        return cache_size;
    }



}