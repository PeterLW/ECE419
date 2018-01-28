package testing;

import common.cache.CacheManager;
import common.disk.DBManager;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

public class CacheTest extends TestCase{
    /* Unit tests for cache*/

    ArrayList<String> cacheStrategy = new ArrayList<String>(){{
        add("FIFO");
        add("LRU");
        add("LFU");
    }};

    DBManager db = new DBManager();

    @Before
    public void setUp() {
        db.clearStorage();
    }

    @Test
    public void testInsert(){
        for (String strategy: cacheStrategy) {
           CacheManager cm = new CacheManager(10, strategy, db);

           for (int i = 0; i < 10; i++) {
               cm.putKV(Integer.toString(i), "b");
           }
           for (int i = 0; i < 10; i++) {
               assertTrue(cm.inCache(Integer.toString(i)));
           }

           String vals[] = new String[10];
           for (int i = 0; i < 10; i++) {
               vals[i] = cm.getKV(Integer.toString(i));
               assertEquals("b", vals[i]);
           }
       }
   }
    @Test
    public void testUpdate(){
        for (String strategy: cacheStrategy) {
            CacheManager cm = new CacheManager(10, strategy, db);

            for (int i = 0; i < 10; i++) {
                assertTrue(cm.putKV(Integer.toString(i), "b"));
            }
            String vals[] = new String[10];
            for (int i = 0; i < 10; i++) {
                vals[i] = cm.getKV(Integer.toString(i));
                assertEquals("b", vals[i]);
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(cm.putKV(Integer.toString(i), "c"));
            }

            String getvals[] = new String[10];
            for (int i = 0; i < 10; i++) {
                getvals[i] = cm.getKV(Integer.toString(i));
                assertEquals("c", getvals[i]);
            }
        }
    }

    @Test
    public void testDelete(){
        for (String strategy: cacheStrategy) {
            CacheManager cm = new CacheManager(10, strategy, db);

            for (int i = 0; i < 10; i++) {
                assertTrue(cm.putKV(Integer.toString(i), "b"));
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(cm.inCache(Integer.toString(i)));
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(cm.deleteRV(Integer.toString(i)));
            }

            for (int i = 0; i < 10; i++) {
                assertFalse(cm.inCache(Integer.toString(i)));
            }

            for (int i = 0; i < 20; i++) {
                assertFalse(cm.deleteRV(Integer.toString(i)));
            }

            for (int i = 0; i < 10; i++) {
                assertFalse(cm.inCache(null));
            }
        }
    }

    @Test
    public void testGet(){
        for (String strategy: cacheStrategy) {
            CacheManager cm = new CacheManager(10, strategy, db);

            for (int i = 0; i < 10; i++) {
                assertTrue(cm.putKV(Integer.toString(i), "b"));
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(cm.inCache(Integer.toString(i)));
            }

            String vals[] = new String[10];
            for (int i = 0; i < 10; i++) {
                vals[i] = cm.getKV(Integer.toString(i));
                assertEquals("b", vals[i]);
            }

            for (int i = 0; i < 10; i++) {
                assertTrue(cm.deleteRV(Integer.toString(i)));
            }

            for (int i = 0; i < 10; i++) {
                assertEquals(null, cm.getKV(Integer.toString(i)));
            }
        }
    }

}
