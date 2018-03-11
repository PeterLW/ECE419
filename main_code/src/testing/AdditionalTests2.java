package testing;

import app_kvServer.ServerStatus;
import app_kvServer.ServerStatusType;
import common.zookeeper.ZNodeMessageStatus;
import ecs.ConfigEntity;
import ecs.ServerNode;
import junit.framework.TestCase;

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

}
