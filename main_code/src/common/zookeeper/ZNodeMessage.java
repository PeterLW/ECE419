package common.zookeeper;

import ecs.ServerNode;

import java.math.BigInteger;

public class ZNodeMessage {
    // when znode is updated, the KVserver will look at this value to determine what the update is for
    public ZNodeMessageStatus zNodeMessageStatus;
    public ServerNode serverNode;

    private BigInteger[] moveDataRange = null;
    private String targetName = null;

    /*
        The targetName can be:
            1 variable:
            a String with value: "ip:port"

            2 variables:
            String targetNameIP
            int targetNamePort
     */

    public ZNodeMessage(ServerNode n, ZNodeMessageStatus ZNodeMessageStatus){
        this.serverNode = n;
        this.zNodeMessageStatus = ZNodeMessageStatus;
    }

    public void setMoveDataParameters(BigInteger[] moveDataRange, String targetName){
        this.moveDataRange = new BigInteger[2];
        this.moveDataRange[0] = moveDataRange[0];
        this.moveDataRange[1] = moveDataRange[1];

        this.targetName = targetName;
    }
}
