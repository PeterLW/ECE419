package testing;

import common.cache.StorageManager;
import common.disk.DBManager;

import junit.framework.TestCase;
//import org.junit.Before;
//import org.junit.Test;

import javax.print.DocFlavor;
import java.util.ArrayList;

public class CacheTest extends TestCase{
    /* Unit tests for cache*/
    private static final String DEFAULT = "DEFAULTDB";

    ArrayList<String> cacheStrategy = new ArrayList<String>(){{
        add("FIFO");
        add("LRU");
        add("LFU");
    }};

    //@Test
    public void testInsert(){
        for (String strategy: cacheStrategy) {
           StorageManager sm = new StorageManager(10, strategy,DEFAULT);
           sm.clearAll();

           for (int i = 0; i < 10; i++) {
               sm.putKV(Integer.toString(i), "b");
           }
           for (int i = 0; i < 10; i++) {
               assertTrue(sm.inCache(Integer.toString(i)));
           }

           String vals[] = new String[10];
           for (int i = 0; i < 10; i++) {
               vals[i] = sm.getKV(Integer.toString(i));
               assertEquals("b", vals[i]);
           }
       }
   }
    //@Test
    public void testUpdate(){
        for (String strategy: cacheStrategy) {
            System.out.println(strategy);
            StorageManager rm = new StorageManager(10, strategy,DEFAULT);
            rm.clearAll();

            for (int i = 0; i < 10; i++) {
                assertTrue(rm.putKV(Integer.toString(i), "b"));
            }

            String val;
            for (int i = 0; i < 10; i++) {
                val = rm.getKV(Integer.toString(i));
                assertEquals("b", val);
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(rm.putKV(Integer.toString(i), "c"));
            }

            for (int i = 0; i < 10; i++) {
                val = rm.getKV(Integer.toString(i));
                assertEquals("c", val);
            }
        }
    }

    //@Test
    public void testDelete(){
        for (String strategy: cacheStrategy) {
            StorageManager sm = new StorageManager(10, strategy,DEFAULT);
            sm.clearAll();

            for (int i = 0; i < 10; i++) {
                assertTrue(sm.putKV(Integer.toString(i), "b"));
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(sm.inCache(Integer.toString(i)));
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(sm.deleteRV(Integer.toString(i)));
            }

            for (int i = 0; i < 10; i++) {
                assertFalse(sm.inCache(Integer.toString(i)));
            }

            for (int i = 0; i < 20; i++) {
                assertFalse(sm.deleteRV(Integer.toString(i)));
            }

            for (int i = 0; i < 10; i++) {
                assertFalse(sm.inCache(null));
            }
        }
    }

    //@Test
    public void testGet(){
        for (String strategy: cacheStrategy) {
            StorageManager sm = new StorageManager(10, strategy,DEFAULT);
            sm.clearAll();

            for (int i = 0; i < 10; i++) {
                assertTrue(sm.putKV(Integer.toString(i), "b"));
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(sm.inCache(Integer.toString(i)));
            }

            String vals[] = new String[10];
            for (int i = 0; i < 10; i++) {
                vals[i] = sm.getKV(Integer.toString(i));
                assertEquals("b", vals[i]);
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(sm.deleteRV(Integer.toString(i)));
            }

            for (int i = 0; i < 10; i++) {
                assertEquals(null, sm.getKV(Integer.toString(i)));
            }
        }
    }

}
