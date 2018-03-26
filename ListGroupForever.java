package common.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 *  this class is for test purposes only
 */
public class ListGroupForever {
    private ZooKeeper zooKeeper;
    private Semaphore semaphore = new Semaphore(1);

    public ListGroupForever(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }


    public void listForever(String groupName)
            throws KeeperException, InterruptedException {
        semaphore.acquire();
        while (true) {
            list(groupName);
            semaphore.acquire();
        }
    }

    private void list(String groupName)
            throws KeeperException, InterruptedException {
        String path = groupName;
        List<String> children = zooKeeper.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    semaphore.release();
                    System.out.println("released");
                }
            }
        });

        if (children.isEmpty()) {
            System.out.printf("No members in group %s\n", groupName);
            return;
        }

        Collections.sort(children);
        System.out.println(children);
        System.out.println("--------------------");
    }
}