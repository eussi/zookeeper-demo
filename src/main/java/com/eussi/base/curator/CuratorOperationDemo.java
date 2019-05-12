package com.eussi.base.curator;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wangxueming on 2018/9/29.
 */
public class CuratorOperationDemo {

    public static void main(String[] args) throws IOException, InterruptedException {
        CuratorFramework curatorFramework = CuratorClientUtils.getInstance();

        String path = "/demo/demo1/demo2";
        try {
            //增
            String result = curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, "123".getBytes());
            System.out.println("创建成功:" + result);

            //查
            Stat stat = new Stat();
            byte[] data = curatorFramework.getData()
                    .storingStatIn(stat)   //将节点状态信息存入stat中
                    .forPath(path);
            System.out.println("节点：" + path + ", \n内容："
                    + new String(data)
                    + ", \nstat:"
                    + stat);

            //改
            stat = curatorFramework.setData().forPath(path, "456".getBytes());
            data = curatorFramework.getData()
                    .forPath(path);
            System.out.println("节点：" + path + ", \n修改后内容："
                    + new String(data)
                    + ", \nstat:"
                    + stat);

            //删
            curatorFramework.delete().deletingChildrenIfNeeded()
                    .forPath(path.substring(0, path.indexOf("/", 2)));
            System.out.println("删除成功！");


        } catch (Exception e) {
            e.printStackTrace();
        }

        /**
         * 异步操作过程
         */
        System.out.println("-----------------------------------------------------------------------------------");
        path = "/async/async1";
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(5);
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .inBackground(new BackgroundCallback() {
                        @Override
                        public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                            System.out.println(Thread.currentThread().getName() + "-->"
                            + "\nresultCode:" + curatorEvent.getResultCode()
                            + "\nType:" + curatorEvent.getType()
                            + "\nPath:" + curatorEvent.getPath());
                            countDownLatch.countDown();
                        }
                    }, executorService)
                    .forPath(path, "123".getBytes());
            System.out.println("创建成功!");
            countDownLatch.await();
            executorService.shutdown();

            //删
            curatorFramework.delete().deletingChildrenIfNeeded()
                    .forPath(path.substring(0, path.indexOf("/", 2)));
            System.out.println("删除成功！");


        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("-----------------------------------------------------------------------------------");
        /**
         * 事务操作
         */

        try {
            Collection<CuratorTransactionResult> results = curatorFramework.inTransaction().create().forPath("/aaa") //报错，不存在，并且第一个也不会成功添加
                    .and().setData().forPath("/xxxx", "333".getBytes())
                    .and().commit();
            for(CuratorTransactionResult result: results) {
                System.out.println(result.getForPath() + "-->" + result.getType());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
