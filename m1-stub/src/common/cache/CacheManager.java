package common.cache;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import common.disk.DBManager;

public class CacheManager {

    private static final Logger logger = Logger.getLogger(CacheManager.class);
    private static int cache_size;
    private static String strategy;
    private static CacheStructure cacheStructure = null;
    private static DBManager dbManager = null;

    static {
        try {
            new logger.LogSetup("logs/storage.log", Level.INFO);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CacheManager(int size, String cache_strategy, DBManager dbManager) {
        cache_size = size;
        strategy = cache_strategy;
        dbManager = dbManager;

        if(cache_strategy.equals("FIFO"))
            cacheStructure = new FIFO(size, dbManager);
        else if(cache_strategy.equals("LFU"))
            cacheStructure = new LFU(size, dbManager);
        else if(cache_strategy.equals("LRU"))
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


}