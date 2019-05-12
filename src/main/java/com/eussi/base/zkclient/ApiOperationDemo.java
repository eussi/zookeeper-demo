package com.eussi.base.zkclient;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;
import java.util.List;

/**
 * Created by wangxueming on 2018/9/29.
 */
public class ApiOperationDemo{
    public final static String CONNECTIONSTRING =  "192.168.198.201:2181," +
            "192.168.198.202:2181," +
            "192.168.198.203:2181," +
            "192.168.198.204:2181";

    public static void main(String[] args) throws IOException, InterruptedException {
        ZkClient zkClient = new ZkClient(CONNECTIONSTRING, 10000);
        System.out.println("connected->" + zkClient);

        String path = "/zkClient";
        zkClient.createEphemeral(path, "zkClient node");
        System.out.println("创建节点并写入值：" + zkClient.readData(path));
        Thread.sleep(2000);

        //递归创建节点
        String cascadePath = "/test/test1/test2";
        zkClient.createPersistent(cascadePath, true);
        System.out.println("创建节点：" + cascadePath);
        Thread.sleep(2000);

        zkClient.subscribeChildChanges(cascadePath, new IZkChildListener() {
            @Override
            public void handleChildChange(String s, List<String> list) throws Exception {
                System.out.println("handleChildChange-->子节点：" + s + ", list:" + list);
            }
        });

        zkClient.createPersistent(cascadePath + "/test3");
        System.out.println("创建节点：" + cascadePath + "/test3");
        Thread.sleep(2000);

        //获取子节点
        List<String> nodes = zkClient.getChildren(cascadePath.substring(0, cascadePath.indexOf("/", 1)));
        System.out.println("获取子节点：" + nodes);
        Thread.sleep(2000);

        //级联删除
        zkClient.deleteRecursive(cascadePath.substring(0, cascadePath.indexOf("/", 1)));
        System.out.println("级联删除节点：" + cascadePath.substring(0, cascadePath.indexOf("/", 1)));
        Thread.sleep(2000);

        zkClient.subscribeDataChanges(path, new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {
                System.out.println("handleDataChange-->节点：" + s + "，改变后值：" + o);
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                System.out.println("handleDataDeleted-->删除节点：" + s);
            }
        });

        //修改触发时间
        zkClient.writeData(path, "new zkClient node");
        System.out.println("修改节点：" + path);
        Thread.sleep(2000);

        //删除触发时间
        zkClient.delete(path);
        System.out.println("删除节点：" + path);
        Thread.sleep(2000);


    }
}
