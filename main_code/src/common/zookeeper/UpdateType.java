package common.zookeeper;

public enum UpdateType {
    /*
        When data in a znode is updated by the ECS Client, the following changes are possible:
     */
    NEW_ZNODE,
    HASH_RANGE_UPDATE,
    START_SERVER,
    STOP_SERVER,
    SHUTDOWN_SERVER
}
