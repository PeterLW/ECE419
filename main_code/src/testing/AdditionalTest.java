package testing;

import com.google.gson.Gson;
import common.cache.StorageManager;
import common.disk.DBManager;
import common.messages.KVMessage;
import common.messages.Message;
import org.junit.Test;

import junit.framework.TestCase;

import java.io.File;
import java.nio.file.Paths;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3

	@Test
	public void testMessage(){
		/*
		 * ensures Message is serialized properly
		 */
		Message m = new Message(KVMessage.StatusType.GET, 1, 2, "11", "vaaaa");
		Gson gson = new Gson();
		String json = gson.toJson(m);

		String expected = "{\"status\":\"GET\",\"seq\":2,\"clientId\":1,\"key\":\"11\",\"value\":\"vaaaa\"}";
		assertEquals(json,expected);
	}


	@Test
	public void testFIFO(){
		/* FIFO implementation
		 * ensures correct keys are in cache
		 */
		StorageManager sm = new StorageManager(5,"FIFO");
		sm.clearAll();

		for (int i = 0; i < 10; i ++) {
			sm.putKV(Integer.toString(i), "b");
		}

		assertTrue(sm.inCache("7"));
		assertTrue(sm.inCache("8"));
		assertTrue(sm.inCache("9"));
		assertTrue(sm.inCache("6"));
		assertTrue(sm.inCache("5"));
	}

	@Test
	public void testLRU(){
		/* LRU implementation
		 * ensures correct keys are in cache
		 */
		StorageManager sm = new StorageManager(5,"LRU");
		sm.clearAll();

		for (int i = 0; i < 10; i ++) {
			sm.putKV(Integer.toString(i), "b");
		}// 5 6 7 8 9

		for (int i = 7; i > 2; i --) {
			sm.printCacheKeys();
			sm.putKV(Integer.toString(i), "b");
		} // 5 6 7 4 3

		assertTrue(sm.inCache("7"));
		assertTrue(sm.inCache("5"));
		assertTrue(sm.inCache("6"));
		assertTrue(sm.inCache("4"));
		assertTrue(sm.inCache("3"));
	}

	@Test
	public void testLFU(){
		/* LFU implementation
		 * ensures correct keys are in cache
		 */
		StorageManager sm = new StorageManager(5,"LFU");
		sm.clearAll();

		for (int i = 0; i < 10; i ++) {
			sm.putKV(Integer.toString(i), "b");
		}// 5 6 7 8 9

        sm.putKV("9", "b");
		sm.putKV("9", "c");
		sm.putKV("5", "d");
        sm.getKV("6");

        sm.getKV("1");
		sm.getKV("2");

        assertTrue(sm.inCache("9"));
		assertTrue(sm.inCache("5"));
		assertTrue(sm.inCache("6"));
		assertTrue(sm.inCache("1"));
		assertTrue(sm.inCache("2"));

		sm.getKV("2");
		sm.getKV("2");
		// keys: 9 6 5 1 2
		// freq: 3 2 2 1 3

        sm.getKV("4");
		sm.getKV("4");
		sm.getKV("8");
		// for ties should use FIFO

        assertTrue(sm.inCache("4"));
		assertTrue(sm.inCache("9"));
		assertTrue(sm.inCache("8"));
		assertTrue(sm.inCache("6"));
		assertTrue(sm.inCache("2"));
	}
	
	@Test
	public void testWriteThroughCache() {
		/*
		 * ensures KVs are properly written to database through cache structure
		 */
		StorageManager sm = new StorageManager(5,"LFU");
		sm.clearAll();

		sm.putKV("a2","b");
		sm.putKV("a1","b");
		sm.putKV("a3","b");

		assertTrue(sm.inDatabase("a2"));
		assertTrue(sm.inDatabase("a1"));
		assertTrue(sm.inDatabase("a3"));
	}

	@Test
	public void testInsertionPolicy(){
		/*
		 * Ensures that cache inserts into free slots after free slots created after deleteKV
		 */
		final StorageManager sm = new StorageManager(5,"LRU");
		sm.clearAll();

		for (int i = 0; i < 8; i ++) {
			sm.putKV(Integer.toString(i), "b");
		}// 3 4 5 6 7

		for (int i = 5; i > 0; i --) {
			sm.putKV(Integer.toString(i), "b");
		} // 3 4 5 2 1
		sm.printCacheKeys();

		assertTrue(sm.inCache("3"));
		assertTrue(sm.inCache("4"));
		assertTrue(sm.inCache("5"));
		assertTrue(sm.inCache("2"));
		assertTrue(sm.inCache("1"));

		sm.deleteRV("3");
		sm.deleteRV("1");

		assertFalse(sm.inCache("3"));
		assertFalse(sm.inCache("1"));

		sm.putKV("10", "a");
		sm.putKV("11", "a");
		sm.putKV("12", "a");

		assertTrue(sm.inCache("11"));
		assertTrue(sm.inCache("12"));
		assertTrue(sm.inCache("10"));
		assertTrue(sm.inCache("2"));
		assertTrue(sm.inCache("4"));
	}

	@Test
	public void testDeleteKV() {
		/* tests to ensure that KV are deleted
		 * from database and cache
		 */
		StorageManager sm = new StorageManager(5,"LRU");
		sm.clearAll();

		sm.putKV("a1","b");
		sm.putKV("a2","b");
		sm.putKV("a3","b");
		sm.putKV("a4","b");
		sm.putKV("a5","b");
		sm.putKV("a6","b");
		sm.putKV("a7","b");
		// 3 4 5 6 7

		sm.deleteRV("a6");
		sm.deleteRV("a7");
		sm.deleteRV("a1");

		assertTrue(sm.inCache("a5"));
		assertTrue(sm.inCache("a4"));
		assertTrue(sm.inCache("a3"));

		assertFalse(sm.inCache("a1"));
		assertFalse(sm.inDatabase("a7"));
		assertFalse(sm.inDatabase("a6"));
		assertFalse(sm.inDatabase("a1"));

	}

	@Test
	public void testdB() {
		/* tests that database creates persistent file storage
		 * and deletes when called
		 */
		DBManager db = new DBManager();
		db.clearStorage();

		db.storeKV("a2","b");
		db.storeKV("a3","b");

		File rootDB = new File("DBRoot");
		File a2 = new File(String.valueOf(Paths.get("DBRoot","a2")));
		File a3 = new File(String.valueOf(Paths.get("DBRoot","a2")));
		assertTrue(rootDB.exists());
		assertTrue(a2.exists());
		assertTrue(a3.exists());

		db.clearStorage();
		assertTrue(rootDB.exists());
		assertFalse(a2.exists());
		assertFalse(a3.exists());
	}

}
