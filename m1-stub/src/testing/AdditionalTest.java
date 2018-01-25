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
	public void testStub() {
		assertTrue(true);
	}

	@Test
	public void testMessage(){
		Message m = new Message(KVMessage.StatusType.GET, 1, 2, "11", "vaaaa");
		Gson gson = new Gson();
		String json = gson.toJson(m);

		String expected = "{\"status\":\"GET\",\"seq\":2,\"clientId\":1,\"key\":\"11\",\"value\":\"vaaaa\"}";

		assertEquals(json,expected);
	}


	@Test
	public void testFIFO(){
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
	public void testdB() {
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
