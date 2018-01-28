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
		DBManager db = new DBManager();
		CacheManager cm = new CacheManager(5,"FIFO",db);

		for (int i = 0; i < 10; i ++) {
			cm.putKV(Integer.toString(i), "b");
		}

		assertTrue(cm.inCache("7"));
		assertTrue(cm.inCache("8"));
		assertTrue(cm.inCache("9"));
		assertTrue(cm.inCache("6"));
		assertTrue(cm.inCache("5"));
	}

	@Test
	public void testLRU(){
		/* LRU implementation
		 * ensures correct keys are in cache
		 */
		DBManager db = new DBManager();
		CacheManager cm = new CacheManager(5,"LRU",db);

		for (int i = 0; i < 10; i ++) {
			cm.putKV(Integer.toString(i), "b");
		}// 5 6 7 8 9

		for (int i = 7; i > 2; i --) {
			cm.printCacheKeys();
			cm.putKV(Integer.toString(i), "b");
		} // 5 6 7 4 3

		assertTrue(cm.inCache("7"));
		assertTrue(cm.inCache("5"));
		assertTrue(cm.inCache("6"));
		assertTrue(cm.inCache("4"));
		assertTrue(cm.inCache("3"));
	}

	@Test
	public void testLFU(){
		/* LFU implementation
		 * ensures correct keys are in cache
		 */
		DBManager db = new DBManager();
		CacheManager cm = new CacheManager(5,"LFU",db);

		for (int i = 0; i < 10; i ++) {
			cm.putKV(Integer.toString(i), "b");
		}// 5 6 7 8 9
		cm.putKV("9", "b");
		cm.putKV("9", "c");
		cm.putKV("5", "d");
		cm.getKV("6");

		cm.getKV("1");
		cm.getKV("2");

		assertTrue(cm.inCache("9"));
		assertTrue(cm.inCache("5"));
		assertTrue(cm.inCache("6"));
		assertTrue(cm.inCache("1"));
		assertTrue(cm.inCache("2"));

		cm.getKV("2");
		cm.getKV("2");
		// 9 6 5 1 2
		// 3 2 2 1 3

		cm.getKV("4");
		cm.getKV("4");
		cm.getKV("8");
		// for ties should use FIFO

		assertTrue(cm.inCache("4"));
		assertTrue(cm.inCache("9"));
		assertTrue(cm.inCache("8"));
		assertTrue(cm.inCache("6"));
		assertTrue(cm.inCache("2"));
	}
	
	@Test
	public void testWriteThroughCache() {
		/*
		 * ensures KVs are properly written to database through cache structure
		 */
		DBManager db = new DBManager();
		CacheManager cm = new CacheManager(5,"LFU",db);

		cm.putKV("a2","b");
		cm.putKV("a1","b");
		cm.putKV("a3","b");

		assertTrue(db.isExists("a2"));
		assertTrue(db.isExists("a1"));
		assertTrue(db.isExists("a3"));

	}

	@Test
	public void testInsertionPolicy(){
		/*
		 * Ensures that cache inserts into free slots after free slots created after deleteKV
		 */
		final DBManager db = new DBManager();
		final CacheManager cm = new CacheManager(5,"LRU",db);

		for (int i = 0; i < 8; i ++) {
			cm.putKV(Integer.toString(i), "b");
		}// 3 4 5 6 7

		for (int i = 5; i > 0; i --) {
			cm.putKV(Integer.toString(i), "b");
		} // 3 4 5 2 1
		cm.printCacheKeys();

		assertTrue(cm.inCache("3"));
		assertTrue(cm.inCache("4"));
		assertTrue(cm.inCache("5"));
		assertTrue(cm.inCache("2"));
		assertTrue(cm.inCache("1"));

		cm.deleteRV("3");
		cm.deleteRV("1");

		assertFalse(cm.inCache("3"));
		assertFalse(cm.inCache("1"));

		cm.putKV("10", "a");
		cm.putKV("11", "a");
		cm.putKV("12", "a");

		assertTrue(cm.inCache("11"));
		assertTrue(cm.inCache("12"));
		assertTrue(cm.inCache("10"));
		assertTrue(cm.inCache("2"));
		assertTrue(cm.inCache("4"));
	}

	@Test
	public void testDeleteKV() {
		/* tests to ensure that KV are deleted
		 * from database and cache
		 */
		DBManager db = new DBManager();
		CacheManager cm = new CacheManager(5,"LRU",db);

		cm.putKV("a1","b");
		cm.putKV("a2","b");
		cm.putKV("a3","b");
		cm.putKV("a4","b");
		cm.putKV("a5","b");
		cm.putKV("a6","b");
		cm.putKV("a7","b");
		// 3 4 5 6 7

		cm.deleteRV("a6");
		cm.deleteRV("a7");
		cm.deleteRV("a1");

		assertTrue(cm.inCache("a5"));
		assertTrue(cm.inCache("a4"));
		assertTrue(cm.inCache("a3"));

		assertFalse(cm.inCache("a1"));
		assertFalse(db.isExists("a7"));
		assertFalse(db.isExists("a6"));
		assertFalse(db.isExists("a1"));

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
