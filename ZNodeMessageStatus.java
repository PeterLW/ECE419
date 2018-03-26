package common.zookeeper;

public enum ZNodeMessageStatus {
    /*
        When data in a znode is updated by the ECS Client, the following changes are possible:
     */
    NEW_ZNODE,
    NEW_ZNODE_RECEIVE_DATA,
    REMOVE_ZNODE_SEND_DATA,
    MOVE_DATA_SENDER,
    MOVE_DATA_RECEIVER,
    START_SERVER,
    STOP_SERVER,
    SHUTDOWN_SERVER

}
