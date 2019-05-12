package com.eussi.base.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by wangxueming on 2018/9/30.
 */
public class CuratorClientUtils {
    public final static String CONNECTIONSTRING =  "192.168.198.201:2181," +
            "192.168.198.202:2181," +
            "192.168.198.203:2181," +
            "192.168.198.204:2181";
    public static CuratorFramework getInstance() {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(CONNECTIONSTRING, 5000, 5000,
                new ExponentialBackoffRetry(1000, 3)); //ExponentialBackoffRetry衰减重试

        curatorFramework.start();
        System.out.println("success:" + curatorFramework);
        return curatorFramework;
    }
}
