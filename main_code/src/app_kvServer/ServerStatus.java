package app_kvServer;

public enum ServerStatus {
    STARTING, // booting up
    RUNNING, // can respond to clients
    UPDATING_RANGE, // can't respond to clients
    // maybe there's a separate status for when transfering data, I'm not sure...
    STOPPED // there is a stop() function in ecsClient
}
