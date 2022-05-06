package com.wyw.zookeeper.distributeLock;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @author 5yw
 * @date 2021/10/29 10:05
 */
public class Client {
    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        Lock lock1 = new Lock();
        Lock lock2 = new Lock();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("========线程1启动========");
                    lock1.zkLock();

                    lock1.zkUnlock();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        Thread.sleep(3 * 1000);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("========线程2启动========");
                    lock2.zkLock();

                    lock2.zkUnlock();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
