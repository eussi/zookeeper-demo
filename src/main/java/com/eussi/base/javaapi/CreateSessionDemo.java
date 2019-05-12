package com.eussi.base.javaapi;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by wangxueming on 2018/9/29.
 */
public class CreateSessionDemo {
//    public final static String CONNECTIONSTRING =  "192.168.198.201:2181," +
//            "192.168.198.202:2181," +
//            "192.168.198.203:2181," +
//            "192.168.198.204:2181";
    public final static String CONNECTIONSTRING =  "192.168.198.202:2181";
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, InterruptedException {

        ZooKeeper zooKeeper = new ZooKeeper(CONNECTIONSTRING, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    countDownLatch.countDown();
                }
                System.out.println("watchedEvent state: " + watchedEvent.getState());

            }
        });
        countDownLatch.await();

        System.out.println("zooKeeper state: " + zooKeeper.getState());
    }
}
