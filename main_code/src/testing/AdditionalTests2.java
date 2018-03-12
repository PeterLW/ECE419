package testing;

import app_kvServer.ServerStatus;
import app_kvServer.ServerStatusType;
import common.zookeeper.ZNodeMessageStatus;
import ecs.ConfigEntity;
import ecs.ServerManager;
import ecs.ServerNode;
import junit.framework.TestCase;
import org.apache.zookeeper.KeeperException;

public class AdditionalTests2 extends TestCase {

    private void fct(ServerNode n){
        ServerStatus s = new ServerStatus(ZNodeMessageStatus.NEW_ZNODE_RECEIVE_DATA);
        s.setServerStatus(ServerStatusType.MOVE_DATA_SENDER);
        n.setServerStatus(s);
    }

    //@Test
    public void testSettingStatus(){
        ConfigEntity e = new ConfigEntity("Server1","localhost", 50000);
        ServerNode n = new ServerNode(e,100,"LFU");

        ServerStatus s = new ServerStatus(ZNodeMessageStatus.NEW_ZNODE);
        n.setServerStatus(s);
        fct(n);
        assertEquals(0,n.getServerStatus().getTransition().compareTo(ZNodeMessageStatus.NEW_ZNODE_RECEIVE_DATA));
        assertEquals(0,n.getServerStatus().getStatus().compareTo(ServerStatusType.MOVE_DATA_SENDER));
    }

    public void testparseConfigFile(){
        ServerManager sm = new ServerManager();
        boolean exist = sm.getEntityList().isEmpty();

        assertEquals(exist, false);
    }

    //need to run zk to test
    public void testServerManagerAddServer(){
        ServerManager sm = new ServerManager();

        sm.setupNodes(1,  "FIFO", 100);
        boolean empty = sm.getServerMap().isEmpty();
        boolean correctSize = (sm.getServerMap().size() == 1);

        assertEquals(!empty && correctSize, true);
    }

    //need to run zk to test
    public void testServerManagerRemoveServer(){
        ServerManager sm = new ServerManager();

        sm.setupNodes(1,  "FIFO", 100);
        sm.removeNode(sm.getServerMap().entrySet().iterator().next().getKey());

        boolean exist = sm.getServerMap().isEmpty();
        assertEquals(exist, true);
    }

    public void testServerManagerRemoveServers(){
        ServerManager sm = new ServerManager();

        sm.setupNodes(2,  "FIFO", 100);

        sm.removeNode(sm.getServerMap().entrySet().iterator().next().getKey());
        sm.removeNode(sm.getServerMap().entrySet().iterator().next().getKey());

        boolean exist = sm.getServerMap().isEmpty();
        assertEquals(exist, true);
    }
}
