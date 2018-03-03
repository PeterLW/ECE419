package common.zookeeper;

import ecs.ServerNode;

public class ZNodeMessage {
    public UpdateType updateType;
    // when znode is updated, the KVserver will look at this value to determine what the update is for

    public ServerNode serverNode;

    public ZNodeMessage(ServerNode n, UpdateType updateType){
        this.serverNode = n;
        this.updateType = updateType;
    }
}
