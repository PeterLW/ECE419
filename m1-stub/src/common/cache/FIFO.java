package common.cache;
import java.io.IOException;
import java.util.*;
import common.disk.DBManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

public class FIFO implements CacheStructure{
    int size;
    private static LinkedHashMap<String,String> fifo;
    private static Logger logger = Logger.getLogger(FIFO.class);
    private DBManager database_mgr=null;

    static {
        try {
            new logger.LogSetup("logs/storage.log", Level.DEBUG);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FIFO(int size, DBManager database_mgr) {
        this.size = size;
        this.database_mgr = database_mgr;
        this.fifo = new LinkedHashMap<String,String>();
    }

    @Override
    public synchronized boolean putKV(String key, String value){
        if(fifo.size() >= size){
            fifo.remove(fifo.entrySet().iterator().next().getKey());
        }
        fifo.put(key,value);

        //update the disk
        if(!database_mgr.storeKV(key,value)){
            logger.error("Error: failed to update <"+key+","+value+"> to database");
        } else {
            logger.info("<" + key + "," + value + "> has been updated to database");
        }
        return true;
    }

    @Override
    public synchronized String getKV(String key){
        if (fifo.containsKey(key)){
            return fifo.get(key);
        }

        String value = database_mgr.getKV(key);
        if(value == null){
            logger.error("key-value pair of "+key+" does not exist in database");
            return null;
        } else {
            //put the <key,pair> to cache now to avoid accessing disk again
            if (fifo.size() >= size) {
                fifo.remove(fifo.entrySet().iterator().next().getKey());
            }
            fifo.put(key, value);
            return value;
        }
    }

    @Override
    public synchronized boolean deleteKV(String key){
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