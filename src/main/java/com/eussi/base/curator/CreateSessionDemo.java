package com.eussi.base.curator;

import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;

/**
 * Created by wangxueming on 2018/9/29.
 */
public class CreateSessionDemo {
    public final static String CONNECTIONSTRING =  "192.168.198.201:2181," +
            "192.168.198.202:2181," +
            "192.168.198.203:2181," +
            "192.168.198.204:2181";

    public static void main(String[] args) throws IOException, InterruptedException {
        //normal创建方式
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(CONNECTIONSTRING, 5000, 5000,
                new ExponentialBackoffRetry(1000, 3)); //ExponentialBackoffRetry衰减重试

        curatorFramework.start();
        System.out.println("success:" + curatorFramework);

        //fluent风格
        CuratorFramework curatorFramework1 = CuratorFrameworkFactory.builder()
                .connectString(CONNECTIONSTRING)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
//                .namespace("/root")  //表示后续操作都是以此节点为根节点进行操作
                .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
        curatorFramework1.start();
        System.out.println("success:" + curatorFramework1);

    }
}
