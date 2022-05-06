package com.wyw.zookeeper.distributeLock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

/**
 * @author 5yw
 * @date 2021/10/29 10:05
 */
public class Lock {
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int timeOut = 2000;
    private ZooKeeper client;
    private String currentLock;
    private String previousLock;
    private CountDownLatch waitListen;
    private Semaphore lockGetListen;

    public Lock() throws IOException, KeeperException, InterruptedException {
        // 1.获取zk连接
        init();
        // 2.判断根节点/locks是否存在
        Stat stat = client.exists("/locks", false);
        if(stat == null)
            client.create("/locks", "test_lock".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // 3.初始化信号量
        waitListen = new CountDownLatch(1);
        lockGetListen = new Semaphore(0);
    }

    public void init() throws IOException {
        client = new ZooKeeper(connectString, timeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(previousLock)){
                    waitListen.countDown();
                }
            }
        });
    }

    //加锁
    public void zkLock() throws KeeperException, InterruptedException {
        // 1.创建对应的带序号临时节点
        currentLock = client.create("/locks/" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        // 2.判断创建的节点是否是最小的序号节点，如果是则获取到锁，否则，监听他序号的前一个节点
        // 获取locks下所有节点名称
        List<String> locks = client.getChildren("/locks", false);

        if(locks.size() == 1){
            System.out.println("成功获取到锁:" + currentLock);
        }else{
            //根据序号进行排序
            Collections.sort(locks);
            // 获取自己的节点名称以及在子节点中的序号
            currentLock = currentLock.substring("/locks/".length());
            int index =  locks.indexOf(currentLock);
            if(index == -1){
                System.out.println("数据异常");
            }else if(index == 0) {
                System.out.println("成功获取到锁:" + currentLock);
            } else {
                //监听它的前一个节点
                previousLock = "/locks/" + locks.get(index - 1);
                client.getData(previousLock, true, null);

                waitListen.await();
                System.out.println("成功获取到锁:" + currentLock);
            }
            currentLock = "/locks/" + currentLock;
        }
        lockGetListen.release();
    }

    //解锁
    public void zkUnlock() throws KeeperException, InterruptedException {
        lockGetListen.acquire();                //获取到锁之后，继续等待十秒(模拟工作时间)，然后才能释放锁
        Thread.sleep(10 * 1000);

        System.out.println("释放锁:" + currentLock);
        client.delete(currentLock, -1);
    }
}
