package common.zookeeper;

public enum ZNodeMessageStatus {
    /*
        When data in a znode is updated by the ECS Client, the following changes are possible:
     */
    NEW_ZNODE,
    HASH_RANGE_UPDATE,
    MOVE_DATA_SENDER,
    MOVE_DATA_RECEIVER,
    START_SERVER,
    STOP_SERVER,
    SHUTDOWN_SERVER
}
