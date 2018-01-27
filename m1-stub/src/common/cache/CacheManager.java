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
    	
    	String val = "";
    	
    	if(cacheStructure.inCacheStructure(key)){
    		val = cacheStructure.getKV(key);
        }
    	else{
    		if(dbManager.isExists(key)){
    			val = dbManager.getKV(key);
    			cacheStructure.putKV(key, val);
    		}
    		else{
    			return null;
    		}
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
    	
    	boolean success_delete_db = false;
    	if(cacheStructure.inCacheStructure(key)){
    		 cacheStructure.deleteKV(key);
    	}
    	if(dbManager.isExists(key)){
    		success_delete_db = dbManager.deleteKV(key);
    	}
    	return success_delete_db;
        
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