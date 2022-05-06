package com.wyw.zookeeper.Client;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author 5yw
 * @date 2021/10/28 17:32
 */
public class CreateNode {
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int timeOut = 2000;
    private ZooKeeper client;
    @Before
    public void init() throws IOException {
        client = new ZooKeeper(connectString, timeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                List<String> temp = null;//每声明一次，只会监听一次变化，如果想始终监听，需要在init()的new Watcher中写入方法
                try {
                    temp = client.getChildren("/", true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                temp.forEach(t -> System.out.println(t));
            }
        });
    }

    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String node = client.create("/ideaTest", "test_data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void getChild() throws KeeperException, InterruptedException {
        List<String> temp = client.getChildren("/", true);//每声明一次，只会监听一次变化，如果想始终监听，需要在init()的new Watcher中写入方法
        temp.forEach(t -> System.out.println(t));                       // 这里可以写true，执行init()中的watcher方法，或者写new Watcher自定义
    }

    @Test
    public void consistgetChild() throws KeeperException, InterruptedException, IOException {
        init();
        Thread.sleep(Integer.MAX_VALUE);
    }

    @Test
    public void hasNode() throws KeeperException, InterruptedException {
        Stat stat = client.exists("/input", false);
        System.out.println(stat == null);
    }
}
