package app_kvServer;

public enum ServerStatusType {
    INITIALIZE, // initialize state
    RUNNING,
    IDLE,
    MOVE_DATA_SENDER,
    MOVE_DATA_RECEIVER,
    CLOSE // TODO: only used by KVServer to shut down other running threads
}
