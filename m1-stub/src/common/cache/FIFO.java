package common.cache;
import java.util.*;
import common.disk.DBManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

public class FIFO{
    int size;
    private static LinkedHashMap<String,String> fifo;
    private static Logger logger = Logger.getRootLogger();
    private DBManager database_mgr=null;


    public FIFO(int size, DBManager database_mgr) {
        this.size = size;
        this.database_mgr = database_mgr;
        this.fifo = new LinkedHashMap<String,String>();
    }

    public synchronized boolean putKV(String key, String value){
        if(fifo.size() >= size){
            fifo.remove(fifo.entrySet().iterator().next().getKey());
        }
        fifo.put(key,value);
        //update the disk
        if(database_mgr.storeKV(key,value) == false){
            logger.error("Error: failed to update <"+key+","+value+"> to disk");
        }
        else {
            logger.info("<" + key + "," + value + "> has been updated to disk");
        }
        return true;
    }

    public synchronized String getKV(String key){

        if(fifo.containsKey(key) == false) {
            logger.info("key-value pair of "+key+" does not exist in cache");
            String value = database_mgr.getKV(key);
            if(value == null){
                logger.error("key-value pair of "+key+" does not exist in disk");
                return null;
            }
            else {
                //put the <key,pair> to cache now to avoid accessing disk again
                if (fifo.size() >= size) {
                    fifo.remove(fifo.entrySet().iterator().next().getKey());
                }
                fifo.put(key, value);
                return value;
            }
        }
        else
            return fifo.get(key);
    }

    public void clear(){
        fifo.clear();
    }

    public boolean in_fifo(String key){
        return fifo.containsKey(key);
    }

    public int get_cache_size(){
        return size;
    }
}