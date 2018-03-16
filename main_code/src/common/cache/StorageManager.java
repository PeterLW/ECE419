package common.cache;

import org.apache.log4j.Logger;
import common.disk.DBManager;

import java.math.BigInteger;
import java.util.ArrayList;

public class StorageManager {

    private static Logger logger = Logger.getLogger(StorageManager.class);
    private static int cache_size;
    private static String strategy;
    private static CacheStructure cacheStructure = null;
    private static DBManager dbManager = new DBManager();

    public StorageManager(int size, String cache_strategy) {
        cache_size = size;
        strategy = cache_strategy;

        if(cache_strategy.toUpperCase().equals("FIFO")) {
            cacheStructure = new FIFO(size);
        }
        else if(cache_strategy.toUpperCase().equals("LFU")) {
            cacheStructure = new LFU(size);
        }
        else if(cache_strategy.toUpperCase().equals("LRU")) {
            cacheStructure = new LRU(size);
        } else {
            logger.error("Error: Invalid cache strategy !");
        }
    }

    public boolean putKV(String key, String value){
       return (cacheStructure.putKV(key,value) && dbManager.storeKV(key,value));
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

    public void clearCache(){
        cacheStructure.clear();
    }

    public void clearAll(){
        this.clearCache();
        dbManager.clearStorage();
    }

    public boolean inCache(String key){
        return cacheStructure.inCacheStructure(key);
    }

    public boolean inDatabase(String key){ return dbManager.isExists(key); }

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
    	if(cacheStructure.inCacheStructure(key)){
    		 cacheStructure.deleteKV(key);
    	}
    	return dbManager.deleteKV(key);
    }

    public void deleteCacheRV(String key){
        if(cacheStructure.inCacheStructure(key)){
             cacheStructure.deleteKV(key);
        }
        return;
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

    public ArrayList<String> returnKeysInRange(BigInteger range[]){
        return dbManager.returnKeysInRange(range);
    }
}