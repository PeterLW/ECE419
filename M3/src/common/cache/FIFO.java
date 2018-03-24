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

    public FIFO(int size) {
        this.size = size;
        this.fifo = new LinkedHashMap<String,String>();
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
        }
        return true;
    }

    @Override
    public synchronized void clear(){
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