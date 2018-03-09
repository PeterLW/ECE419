package app_kvServer;

public enum ServerStatusType {
    INITIALIZE, // initialize state
    RUNNING,
    IDLE,
    READ_ONLY, // can't respond to clients
    MOVE_DATA_SENDER,
    MOVE_DATA_RECEIVER,
    CLOSE // only used by KVServer to shut down other running threads
}
