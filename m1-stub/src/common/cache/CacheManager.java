package common.cache;
import java.io.IOException;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import common.disk.DBManager;

public class CacheManager {

    private static Logger logger = Logger.getLogger(CacheManager.class);
    private static int cache_size;
    private static String strategy;
    private static CacheStructure cacheStructure = null;
    private static DBManager dbManager = null;

    static {
        try {
            new logger.LogSetup("logs/storage.log", Level.DEBUG);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CacheManager(int size, String cache_strategy, DBManager db_manager) {
        cache_size = size;
        strategy = cache_strategy;
        this.dbManager = db_manager;

        if(cache_strategy.toUpperCase().equals("FIFO"))
            cacheStructure = new FIFO(size, dbManager);
        else if(cache_strategy.toUpperCase().equals("LFU"))
            cacheStructure = new LFU(size, dbManager);
        else if(cache_strategy.toUpperCase().equals("LRU"))
            cacheStructure = new LRU(size, dbManager);
        else
            logger.error("Error: Invalid cache strategy !");
    }

    public boolean putKV(String key, String value){
       return cacheStructure.putKV(key,value);
    }

    public String getKV(String key){
        return cacheStructure.getKV(key);
    }

    public void clear(){
        cacheStructure.clear();
    }

    public boolean inCache(String key){
        return cacheStructure.inCacheStructure(key);
    }

    public boolean doesKeyExist(String key) {

        if(key.isEmpty() || key == null || key.equals("null") || key.equals("NULL")){
            return false;
        }
        System.out.println(key);
        if (inCache(key) || dbManager.isExists(key)){
            return true;
        }
        return false;
    }

    public boolean deleteFromCache(String key){
       return cacheStructure.deleteKV(key);
    }

    public int get_cache_size(){
        return cache_size;
    }

    public String getReplacementPolicy() {
        return strategy;
    }
    public void printCacheKeys() {
        cacheStructure.printCacheKeys();
    }
}