package common.zookeeper;

import app_kvServer.ServerStatus;
import app_kvServer.UpcomingStatusQueue;
import com.google.gson.Gson;
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
    private boolean running = true;

    private Semaphore semaphore = new Semaphore(1);

    public ZookeeperWatcher(String zookeeperHost, int sessionTimeout, String name, UpcomingStatusQueue _upcomingStatusQueue) throws IOException, InterruptedException {
        super(zookeeperHost,sessionTimeout);
        fullPath = ZNODE_HEAD + "/" + name;
        upcomingStatusQueue = _upcomingStatusQueue;
    }

    // TODO: delete Znode

    public ZNodeMessage getZnodeMessage() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(fullPath,false,null);
        String dataString = new String(data);
        ZNodeMessage newNode = gson.fromJson(dataString,ZNodeMessage.class);
        System.out.println("fetch data from znode");
        System.out.println(new String(data));
        return newNode;
    }

    public ServerNode initServerNode() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(fullPath,false,null);
        String dataString = new String(data);
        ZNodeMessage newNode = gson.fromJson(dataString,ZNodeMessage.class);
        return newNode.serverNode;
    }

    public void setServerNode(ServerNode n){
        serverNode = n;
    }

    public void handleDelete() throws InterruptedException, KeeperException {
        this.deleteZNode(fullPath);
        semaphore.release();
        this.running = false;
        super.close();
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

        ZNodeMessage newMessage = gson.fromJson(new String(data),ZNodeMessage.class);
        ServerStatus ss = null;


        // debug
        System.out.println("Data has changed");
        System.out.println(new String(data));

        // in progress
        switch(newMessage.zNodeMessageStatus){
            case MOVE_DATA_RECEIVER:
            case MOVE_DATA_SENDER:
                ss = new ServerStatus(newMessage.zNodeMessageStatus,newMessage.getMoveDataRange(),newMessage.getTargetName(),serverNode.getRange());
                upcomingStatusQueue.addQueue(ss);
                System.out.println("Serverstatus - KVServer: " + gson.toJson(ss));
                break;
            case REMOVE_ZNODE_SEND_DATA:
                ss = new ServerStatus(newMessage.zNodeMessageStatus,newMessage.getMoveDataRange(),newMessage.getTargetName(),null);
                upcomingStatusQueue.addQueue(ss);
                this.handleDelete();
                break;
            default:
                ss = new ServerStatus(newMessage.zNodeMessageStatus);
                upcomingStatusQueue.addQueue(ss);
                break;
        }
    }

    @Override
    public void run() {
        try {
            semaphore.acquire();
            while(running){
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
//        UpcomingStatusQueue upcomingStatusQueue = new UpcomingStatusQueue();
//        ZookeeperWatcher zookeeperWatcher = new ZookeeperWatcher("localhost:2181",10000,"TEST_SERVER_0 localhost", upcomingStatusQueue);
//        ServerNode n = zookeeperWatcher.initServerNode();
//        zookeeperWatcher.setServerNode(n);
//        zookeeperWatcher.run();



    }
}