package common.cache;
import java.util.*;

public class FIFO{
    int size;
    private static LinkedHashMap<String,String> fifo;

    public FIFO(int size) {
        this.size = size;
        fifo = new LinkedHashMap<String,String>();
    }

    public void putKV(String key, String value){
        if(fifo.size() >= size) fifo.remove(fifo.entrySet().iterator().next().getKey());

        fifo.put(key,value);
    }

    public String getKV(String key){
        if(!fifo.containsKey(key)) {
            System.out.println("cannot find key in fifo");
            return null;
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