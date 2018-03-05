package common.zookeeper;

import common.Metadata.Metadata;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class ZookeeperMetaData extends ZookeeperManager {
    private static Logger LOGGER = Logger.getLogger(ZookeeperECSManager.class);
    private static final String METADATA_FULLPATH = ZNODE_HEAD + "/" + ZNODE_METADATA_NODE;

    public ZookeeperMetaData(String zookeeperHost, int sessionTimeout) throws IOException, InterruptedException {
        super(zookeeperHost,sessionTimeout);
    }

    public Metadata getMetadata() throws KeeperException, InterruptedException {
        zooKeeper.sync(METADATA_FULLPATH,null,null);
        byte[] data = zooKeeper.getData(METADATA_FULLPATH,false,null);
        String dataString = new String(data);
        Metadata n = gson.fromJson(dataString,Metadata.class);
        return n;
    }

}