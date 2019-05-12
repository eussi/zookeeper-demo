package com.eussi.base.javaapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangxueming on 2018/9/29.
 */
public class AuthControlDemo implements Watcher {
    public final static String CONNECTIONSTRING =  "192.168.198.201:2181," +
            "192.168.198.202:2181," +
            "192.168.198.203:2181," +
            "192.168.198.204:2181";
    private static ZooKeeper zooKeeper = null;
    private static ZooKeeper zooKeeper1 = null;
    private static Stat stat = new Stat();
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        zooKeeper = new ZooKeeper(CONNECTIONSTRING, 5000, new ApiOperationDemo());
        System.out.println("zooKeeper state: " + zooKeeper.getState());
        Thread.sleep(2000);

//        zooKeeper.addAuthInfo("digest", "root:root".getBytes());
//        zooKeeper.create("/test", "123".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

        List<ACL> acls = new ArrayList<ACL>();
        ACL acl = new ACL(ZooDefs.Perms.CREATE, new Id("digest", "root:root"));
        acls.add(acl);
        zooKeeper.create("/test", "123".getBytes(), acls, CreateMode.PERSISTENT);

        zooKeeper1 = new ZooKeeper(CONNECTIONSTRING, 5000, new ApiOperationDemo());
        System.out.println("zooKeeper1 state: " + zooKeeper1.getState());
        Thread.sleep(2000);
        zooKeeper1.delete("/test", -1);


    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        if(Event.EventType.None==watchedEvent.getType() && null == watchedEvent.getPath()) {
            System.out.println(watchedEvent.getType() + ":" + watchedEvent.getState() + "-->" + watchedEvent.getType());
        }

    }
}
