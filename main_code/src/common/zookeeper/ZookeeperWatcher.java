package common.zookeeper;

import app_kvServer.ServerStatus;
import app_kvServer.ServerStatusType;
import app_kvServer.UpcomingStatusQueue;
import ecs.ServerNode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.concurrent.Semaphore;

public class ZookeeperWatcher extends ZookeeperMetaData implements Runnable {
    private static Logger LOGGER = Logger.getLogger(ZookeeperECSManager.class);
    private static ServerNode serverNode;
    private static UpcomingStatusQueue upcomingStatusQueue;
    private static String fullPath = null;

    private Semaphore semaphore = new Semaphore(1);

    public ZookeeperWatcher(String zookeeperHost, int sessionTimeout, String name, UpcomingStatusQueue _upcomingStatusQueue) throws IOException, InterruptedException {
        super(zookeeperHost,sessionTimeout);
        fullPath = ZNODE_HEAD + "/" + name;
        upcomingStatusQueue = _upcomingStatusQueue;
    }

    public ServerNode initServerNode() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(fullPath,false,null);
        String dataString = new String(data);
        ZNodeMessage newNode = gson.fromJson(dataString,ZNodeMessage.class);
        return newNode.serverNode;
    }

    public void setServerNode(ServerNode n){
        this.serverNode = n;
    }

    private void getNewData() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(fullPath, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    semaphore.release();
                }
            }
        }, null);

        ZNodeMessage temp = gson.fromJson(new String(data),ZNodeMessage.class);
        ServerStatus ss = null;

        // in progress
        switch(temp.zNodeMessageStatus){
            case MOVE_DATA_RECEIVER:
            case MOVE_DATA_SENDER:
                // debug
                System.out.println("Data has changed");
                System.out.println(new String(data));

                ss = new ServerStatus(temp.zNodeMessageStatus,temp.getMoveDataRange(),temp.getTargetName());
                upcomingStatusQueue.addQueue(ss);
                break;
            default:
                System.out.println("Data has changed");
                System.out.println(new String(data));
                ss = new ServerStatus(temp.zNodeMessageStatus);
                upcomingStatusQueue.addQueue(ss);
        }

        boolean exitLoop = false;
        while(upcomingStatusQueue.peakQueue() != null && !exitLoop){ // TODO: this actually needs to be in main... ><
            // TODO: this part doesn't really work.. I'll explain why tomorrow.. but I think this loop needs to be in
            // another thread, so I might move this in main KVServer.java & that means we may make a small change...
            // we'll discuss tmr. I guess.
            ServerStatus next = upcomingStatusQueue.peakQueue();
            ServerStatus curr = serverNode.getServerStatus();

            boolean proceed = true;

            switch (curr.getStatus()){
                case INITIALIZE:
                    if (next.getTransition() == ZNodeMessageStatus.START_SERVER){
                        next.setServerStatus(ServerStatusType.RUNNING);
                        serverNode.setServerStatus(next);
                    }
                    break;
                case RUNNING:
                    if (next.getTransition() == ZNodeMessageStatus.STOP_SERVER){
                        next.setServerStatus(ServerStatusType.IDLE);
                        serverNode.setServerStatus(next);
                    } else if (next.getTransition() == ZNodeMessageStatus.LOCK_WRITE){
                        next.setServerStatus(ServerStatusType.READ_ONLY);
                        serverNode.setServerStatus(next);
                    }
                case IDLE:
                    if (next.getTransition() == ZNodeMessageStatus.START_SERVER){
                        next.setServerStatus(ServerStatusType.RUNNING);
                        serverNode.setServerStatus(next);
                    }
                case READ_ONLY:
                    if (next.getTransition() == ZNodeMessageStatus.UNLOCK_WRITE){
                        next.setServerStatus(ServerStatusType.RUNNING);
                        serverNode.setServerStatus(next);
                    } else if (next.getTransition() == ZNodeMessageStatus.MOVE_DATA_RECEIVER){
                        next.setMoveRangeStatus(ServerStatusType.MOVE_DATA_RECEIVER,temp.getMoveDataRange(),temp.getTargetName());
                        serverNode.setServerStatus(next);
                    } else if (next.getTransition() == ZNodeMessageStatus.MOVE_DATA_SENDER){
                        next.setMoveRangeStatus(ServerStatusType.MOVE_DATA_SENDER,temp.getMoveDataRange(),temp.getTargetName());
                        serverNode.setServerStatus(next);
                    }
                case MOVE_DATA_RECEIVER:
                case MOVE_DATA_SENDER:
                    if (curr.isReady()){
                        next.setServerStatus(ServerStatusType.READ_ONLY);
                        serverNode.setServerStatus(next);
                        proceed = false;
                    }
                case CLOSE:
                    proceed = false;
                    exitLoop = true;
            }

            if (proceed){
                upcomingStatusQueue.popQueue();
            }
        }

    }

    @Override
    public void run() {
        try {
            semaphore.acquire();
            while(true){
                try {
                    getNewData();
                    semaphore.acquire();
                } catch (KeeperException | InterruptedException e) {
                    LOGGER.error("Failed to get data from znode", e);
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire semaphore, ",e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        UpcomingStatusQueue upcomingStatusQueue = new UpcomingStatusQueue();
        ZookeeperWatcher zookeeperWatcher = new ZookeeperWatcher("localhost:2181",10000,"TEST_SERVER_0 localhost", upcomingStatusQueue);
        ServerNode n = zookeeperWatcher.initServerNode();
        zookeeperWatcher.setServerNode(n);
        zookeeperWatcher.run();
    }
}