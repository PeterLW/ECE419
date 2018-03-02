package common.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZookeeperWatcher extends ZookeeperManager implements Runnable {
    private static Logger LOGGER = Logger.getLogger(ZookeeperECSManager.class);

    public ZookeeperWatcher(String zookeeperHost, int sessionTimeout) throws IOException, InterruptedException {
        super(zookeeperHost,sessionTimeout);
        
    }

    @Override
    public void run() {

    }
}