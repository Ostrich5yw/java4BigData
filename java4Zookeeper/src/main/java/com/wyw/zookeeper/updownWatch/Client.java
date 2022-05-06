package com.wyw.zookeeper.updownWatch;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 5yw
 * @date 2021/10/28 21:53
 */
@Slf4j
public class Client {
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int timeOut = 2000;
    private ZooKeeper client;
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        Client client = new Client();
        // 1. 获取zk连接
        client.init();
        // 2. 监听/servers 下面子节点的动态变化
//        client.getServerList();       //init 已经启动了getServerList方法
        // 3. 获取相应服务
        client.doJob();
    }

    public void init() throws IOException {
        client = new ZooKeeper(connectString, timeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {//getChildren如果第二个参数为true，默认走这里的方法
                try {
                    getServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void getServerList() throws KeeperException, InterruptedException {
        List<String> children = client.getChildren("/servers", true);
        ArrayList<String> servers = new ArrayList<>();
        for(String child : children){
            byte[] data = client.getData("/servers/" + child, false, null);
            servers.add("Name: " + child + "  Val: " + new String(data));
        }
        System.out.println("====================服务器节点更新====================");
        servers.forEach(t -> System.out.println(t));
    }

    public void doJob() throws InterruptedException {
        Thread.sleep(Integer.MAX_VALUE);
    }
}
