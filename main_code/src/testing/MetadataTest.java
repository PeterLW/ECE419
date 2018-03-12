package testing;

import common.metadata.Metadata;
import junit.framework.TestCase;

public class MetadataTest extends TestCase {


    public void testMetadataWrapAround(){
        Metadata m = new Metadata();

        m.addServer("a");
        // 1695523700196324017305827155985872649
        m.addServer("d");
        // 195289424170611159128911017612795795343
        m.addServer("b");
        // 173422077530204247440288476180261147053
        m.addServer("e");
        // 299611584147932843547128611849858313266

        // 333324391474467668306539665063131100439
        String serverName = m.findServer("testkey2");

        assertEquals("a",serverName);
    }

    public void testMetadataFindServer(){
        Metadata m = new Metadata();

        m.addServer("a");
        // 1695523700196324017305827155985872649
        m.addServer("d");
        // 195289424170611159128911017612795795343
        m.addServer("b");
        // 173422077530204247440288476180261147053
        m.addServer("e");
        // 299611584147932843547128611849858313266

        // 45335050332576958438065669921095352181
        String serverName = m.findServer("testkey");

        assertEquals("d",serverName);
    }

    public void testMetadataRemove(){
        Metadata m = new Metadata();

        m.addServer("a");
        // 1695523700196324017305827155985872649
        m.addServer("d");
        // 195289424170611159128911017612795795343
        m.addServer("b");
        // 173422077530204247440288476180261147053
        m.addServer("e");
        // 299611584147932843547128611849858313266

        m.removeServer("d");

        // 45335050332576958438065669921095352181
        String serverName = m.findServer("testkey");

        assertEquals("b",serverName);
    }

    public void testMetadataFindSuccessor(){
        Metadata m = new Metadata();

        m.addServer("a");
        // 1695523700196324017305827155985872649
        m.addServer("d");
        // 195289424170611159128911017612795795343

        String serverName = m.getSuccessor("a");;
        assertEquals("d",serverName);
    }
}
