package testing;

import com.google.gson.Gson;
import common.messages.KVMessage;
import common.messages.Message;
import org.junit.Test;

import junit.framework.TestCase;

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
	public void testDbManager(){
		
	}
}
