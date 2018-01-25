package testing;

import com.google.gson.Gson;
import common.cache.CacheManager;
import common.disk.DBManager;
import common.messages.KVMessage;
import common.messages.Message;
import org.junit.Test;

import junit.framework.TestCase;

import java.io.File;
import java.nio.file.Paths;

public class CacheTest extends TestCase{

    public void testInsert(){
       DBManager db = new DBManager();
       CacheManager cm = new CacheManager(10,"LFU",db);

       for (int i = 0; i < 10; i ++) {
           cm.putKV(Integer.toString(i), "b");
       }
       for (int i = 0; i < 10; i ++) {
           assertTrue(cm.inCache(Integer.toString(i)));
       }

       String vals[] = new String[10];
       for (int i = 0; i < 10; i ++) {
           vals[i] = cm.getKV(Integer.toString(i));
           assertEquals("b",vals[i]);
       }
   }

    public void testUpdate(){
        DBManager db = new DBManager();
        CacheManager cm = new CacheManager(10,"LFU",db);

        for (int i = 0; i < 10; i ++) {
            assertTrue(cm.putKV(Integer.toString(i), "b"));
        }
        String vals[] = new String[10];
        for (int i = 0; i < 10; i ++) {
            vals[i] = cm.getKV(Integer.toString(i));
            assertEquals("b",vals[i]);
        }

        for (int i = 0; i < 10; i ++) {
            assertTrue(cm.putKV(Integer.toString(i), "c"));
        }

        String getvals[] = new String[10];
        for (int i = 0; i < 10; i ++) {
            getvals[i] = cm.getKV(Integer.toString(i));
            assertEquals("c",getvals[i]);
        }
    }

    public void testDelete(){
        DBManager db = new DBManager();
        CacheManager cm = new CacheManager(10,"LFU",db);

        for (int i = 0; i < 10; i ++) {
            assertTrue(cm.putKV(Integer.toString(i), "b"));
        }

        for (int i = 0; i < 10; i ++) {
            assertTrue(cm.inCache(Integer.toString(i)));
        }

        for (int i = 0; i < 10; i ++) {
            assertTrue(cm.deleteFromCache(Integer.toString(i)));
        }

        for (int i = 0; i < 10; i ++) {
            assertFalse(cm.inCache(Integer.toString(i)));
        }

        for (int i = 0; i < 20; i ++) {
            assertFalse(cm.deleteFromCache(Integer.toString(i)));
        }
    }

    public void testGet(){
        DBManager db = new DBManager();
        CacheManager cm = new CacheManager(10,"LFU",db);

        for (int i = 0; i < 10; i ++) {
            assertTrue(cm.putKV(Integer.toString(i), "b"));
        }

        for (int i = 0; i < 10; i ++) {
            assertTrue(cm.inCache(Integer.toString(i)));
        }

        String vals[] = new String[10];
        for (int i = 0; i < 10; i ++) {
            vals[i] = cm.getKV(Integer.toString(i));
            assertEquals("b",vals[i]);
        }

        for (int i = 0; i < 10; i ++) {
           assertTrue(cm.deleteFromCache(Integer.toString(i)));
        }

        for (int i = 0; i < 10; i ++) {
            assertEquals(null,cm.getKV(Integer.toString(i)));
        }
    }

}
