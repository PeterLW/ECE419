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

	// Message Class
//	@Test
//	@DisplayName("Not an actual test, for ensuring small stuff works")
//	public void testMessage(){
//		Message m = new Message(KVMessage.StatusType.GET, 1, 2, "11", "vaaaa");
//		Gson gson = new Gson();
//		String json = gson.toJson(m);
//		System.out.println(json);
//		assertEquals(0,1);
//	}
}
