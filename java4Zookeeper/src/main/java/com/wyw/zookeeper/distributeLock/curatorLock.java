package com.wyw.zookeeper.distributeLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author 5yw
 * @date 2021/10/29 22:30
 */
public class curatorLock {
    public static void main(String[] args){
        InterProcessMutex lock1 = new InterProcessMutex(getCuCuratorFramework(), "/locks");
        InterProcessMutex lock2 = new InterProcessMutex(getCuCuratorFramework(), "/locks");

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.acquire();
                    System.out.println("lock1获取到锁");
                    Thread.sleep(3 * 1000);
                    lock1.release();
                    System.out.println("lock1释放锁");
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.acquire();
                    System.out.println("lock2获取到锁");
                    Thread.sleep(3 * 1000);
                    lock2.release();
                    System.out.println("lock2释放锁");
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    private static CuratorFramework getCuCuratorFramework(){
        ExponentialBackoffRetry policy = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("hadoop102:2181,hadoop103:2181,hadoop104:2181")
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(2000)
                .retryPolicy(policy).build();

        System.out.println("zookeeper启动成功");
        client.start();
        return client;
    }
}
