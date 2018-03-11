package testing;

import app_kvServer.ServerStatus;
import app_kvServer.ServerStatusType;
import common.zookeeper.ZNodeMessage;
import common.zookeeper.ZNodeMessageStatus;
import ecs.ConfigEntity;
import ecs.ServerNode;
import junit.framework.TestCase;

public class RandomTest extends TestCase {

    private void fct(ServerNode n){
        System.out.println(n.getServerStatus().getTransition().name());

        n.getServerStatus().transition = ZNodeMessageStatus.SHUTDOWN_SERVER;

        ServerStatus s = new ServerStatus(ZNodeMessageStatus.NEW_ZNODE_RECEIVE_DATA);
        s.setServerStatus(ServerStatusType.MOVE_DATA_SENDER);
        n.setServerStatus(s);
    }

    //@Test
    public void testStat(){
        ConfigEntity e = new ConfigEntity("Server1","localhost", 50000);
        ServerNode n = new ServerNode(e,100,"LFU");

        ServerStatus s = new ServerStatus(ZNodeMessageStatus.NEW_ZNODE);
        n.setServerStatus(s);
        fct(n);
        System.out.println(n.getServerStatus().getTransition().name());
        System.out.println(n.getServerStatus().getStatus().name());

    }
}
