package com.wyw.zookeeper.updownWatch;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author 5yw
 * @date 2021/10/28 21:53
 */
@Slf4j
public class Server {
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int timeOut = 2000;
    private ZooKeeper client;
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        Server server = new Server();
        // 1. 获取zk连接
        server.init();
        // 2. 到zk注册服务器
        server.regist("serverMaster");
        // 3. 启动相应服务
        server.doJob();
    }

    public void init() throws IOException {
        client = new ZooKeeper(connectString, timeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }

    public void regist(String serverName) throws KeeperException, InterruptedException {
        //创建的是临时节点，随Zookeeper服务器开关而消失
        String node = client.create("/servers/" + serverName, serverName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(node + "is been created");
    }

    public void doJob() throws InterruptedException {
        Thread.sleep(Integer.MAX_VALUE);
    }
}
