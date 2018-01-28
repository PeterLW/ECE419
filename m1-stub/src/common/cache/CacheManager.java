package common.cache;
import java.io.IOException;

import org.apache.log4j.Logger;
import common.disk.DBManager;

public class CacheManager {

    private static Logger logger = Logger.getLogger(CacheManager.class);
    private static int cache_size;
    private static String strategy;
    private static CacheStructure cacheStructure = null;
    private static DBManager dbManager = null;

    public CacheManager(int size, String cache_strategy, DBManager db_manager) {
        cache_size = size;
        strategy = cache_strategy;
        this.dbManager = db_manager;

        if(cache_strategy.toUpperCase().equals("FIFO")) {
            cacheStructure = new FIFO(size, dbManager);
        }
        else if(cache_strategy.toUpperCase().equals("LFU")) {
            cacheStructure = new LFU(size, dbManager);
        }
        else if(cache_strategy.toUpperCase().equals("LRU")) {
            cacheStructure = new LRU(size, dbManager);
        } else {
            logger.error("Error: Invalid cache strategy !");
        }
    }

    public boolean putKV(String key, String value){
    	
       return cacheStructure.putKV(key,value) && dbManager.storeKV(key, value);
    }

    public String getKV(String key){
    	
    	String val;

    	if(cacheStructure.inCacheStructure(key)){
            val = cacheStructure.getKV(key);
            logger.debug("Getting value from cache <" + key + ", " + val + "> ");
        }
    	else{
            val = dbManager.getKV(key);
            if (val != null) {
                cacheStructure.putKV(key, val);
            }
            logger.debug("Getting value from database <" + key + ", " + val + "> ");
        }
    		
        return val;
    }

    public void clear(){
        cacheStructure.clear();
    }

    public boolean inCache(String key){
        return cacheStructure.inCacheStructure(key);
    }

    public boolean doesKeyExist(String key) {

        if(key.isEmpty() || key == null){
            return false;
        }
        if (inCache(key) || dbManager.isExists(key)){
            return true;
        }
        return false;
    }

    public boolean deleteRV(String key){
    	
    	return cacheStructure.deleteKV(key);
    	/* @Aaron @Peter
    	 * When would you only want to call deleteRV but only delete from cache not db o.o?
    	 * because the cache has a separate function to remove only from cache only
    	 * & deleteRV does successfully delete from database (check it now)
    	 */

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