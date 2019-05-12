package com.eussi.base.javaapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * Created by wangxueming on 2018/9/29.
 */
public class ApiOperationDemo implements Watcher{

    /**
     zooKeeper state: CONNECTING
     触发：None:SyncConnected
     -->None
     zooKeeper state: CONNECTED
     connect finished!
     --------------------------------------------------------------------------
     新建临时节点操作：
     创建/create-ephemeral-test成功：/create-ephemeral-test
     触发：NodeCreated:路径:/create-ephemeral-test创建后的值：[B@398bd686
     --------------------------------------------------------------------------
     修改临时节点操作：
     修改 /create-ephemeral-test 成功！！！
     触发：NodeDataChanged:路径:/create-ephemeral-test改变后的值：[B@45ab6087
     --------------------------------------------------------------------------
     新建持久节点操作：
     创建 /create-test 成功：/create-test
     --------------------------------------------------------------------------
     新建子节点操作：
     触发：NodeCreated:路径:/create-test创建后的值：[B@14387bc2
     创建 /create-test/create-test-child 成功：/create-test
     触发：NodeCreated:路径:/create-test/create-test-child创建后的值：[B@66fdeeec
     触发：NodeChildrenChanged:路径:/create-test子节点创建后的值：[B@2950fcc7
     --------------------------------------------------------------------------
     修改持久子节点操作：
     修改 /create-test/create-test-child 成功！！！
     触发：NodeDataChanged:路径:/create-test/create-test-child改变后的值：[B@4038202
     --------------------------------------------------------------------------
     删除持久子节点操作：
     触发：NodeDeleted:路径:/create-test/create-test-child已删除值
     删除 /create-test/create-test-child 成功！！！
     --------------------------------------------------------------------------
     删除子节点操作：
     触发：NodeDeleted:路径:/create-test已删除值
     删除 /create-test 成功！！！
     */


//    public final static String CONNECTIONSTRING =  "192.168.198.201:2181," +
//            "192.168.198.202:2181," +
//            "192.168.198.203:2181," +
//            "192.168.198.204:2181";
    public final static String CONNECTIONSTRING =  "192.168.198.202:2181";
    private static ZooKeeper zooKeeper = null;
    private static Stat stat = new Stat();
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        zooKeeper = new ZooKeeper(CONNECTIONSTRING, 5000, new ApiOperationDemo());//添加监视
        System.out.println("zooKeeper state: " + zooKeeper.getState());
        Thread.sleep(2000);
        System.out.println("zooKeeper state: " + zooKeeper.getState());

        System.out.println("connect finished!");

        System.out.println("--------------------------------------------------------------------------");
        System.out.println("新建临时节点操作：");


        String path = "/create-ephemeral-test";
        Stat stat = zooKeeper.exists(path, true);//继续监视，exits和getData设置数据监视，而getChildren设置子节点监视
        if(stat==null) {
            //创建节点
            String result = zooKeeper.create(path, "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL); //触发NodeCreated，临时节点下无法新增节点
            System.out.println("创建" + path + "成功：" + result);
            Thread.sleep(2000);
        }

        System.out.println("--------------------------------------------------------------------------");
        System.out.println("修改临时节点操作：");

        //修改节点
        zooKeeper.setData(path, "456".getBytes(), -1); //触发NodeDataChanged
        System.out.println("修改 " + path + " 成功！！！");
        Thread.sleep(2000);

        System.out.println("--------------------------------------------------------------------------");
        System.out.println("新建持久节点操作：");

        path = "/create-test";
        zooKeeper.exists(path, true);//触发NodeCreated
        String result = zooKeeper.create(path, "456".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建 " + path + " 成功：" + result);
        Thread.sleep(2000);

        System.out.println("--------------------------------------------------------------------------");
        System.out.println("新建子节点操作：");

        zooKeeper.getChildren(path, true);//为子节点设置监听

        stat = zooKeeper.exists(path + "/create-test-child", true);//继续监视，exits和getData设置数据监视，而getChildren设置子节点监视
        if(stat==null) {
            zooKeeper.create(path + "/create-test-child", "789".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);//触发NodeCreated、NodeChildrenChanged
            System.out.println("创建 " + path + "/create-test-child" + " 成功：" + result);
            Thread.sleep(2000);
        }

        System.out.println("--------------------------------------------------------------------------");
        System.out.println("修改持久子节点操作：");

        zooKeeper.setData(path + "/create-test-child", "666".getBytes(), -1);//触发NodeDataChanged
        System.out.println("修改 " + path + "/create-test-child" + " 成功！！！");
        Thread.sleep(2000);

        System.out.println("--------------------------------------------------------------------------");
        System.out.println("删除持久子节点操作：");

        zooKeeper.delete(path + "/create-test-child", -1);//触发NodeCreated、NodeChildrenChanged
        System.out.println("删除 " + path + "/create-test-child" + " 成功！！！");
        Thread.sleep(2000);

        System.out.println("--------------------------------------------------------------------------");
        System.out.println("删除子节点操作：");

        zooKeeper.delete(path, -1); //触发NodeDeleted
        System.out.println("删除 " + path + " 成功！！！");
        Thread.sleep(2000);

    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            if(Event.EventType.None==watchedEvent.getType() && null == watchedEvent.getPath()) {
                System.out.println("触发：" + watchedEvent.getType() + ":" + watchedEvent.getState() + "\n\t-->" + watchedEvent.getType());

            } else if (Event.EventType.NodeDataChanged == watchedEvent.getType()) { //节点数据发生变化的时候触发
                try {
                    System.out.println("触发：" + watchedEvent.getType() + ":路径:" + watchedEvent.getPath()
                            + "改变后的值："
                            + zooKeeper.getData(watchedEvent.getPath(), true, stat));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (Event.EventType.NodeChildrenChanged == watchedEvent.getType()) { //子节点的数据变化的时候触发
                try {
                    System.out.println("触发：" + watchedEvent.getType() + ":路径:" + watchedEvent.getPath()
                            + "子节点创建后的值："
                            + zooKeeper.getData(watchedEvent.getPath(), true, stat));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (Event.EventType.NodeCreated == watchedEvent.getType()) {  //
                try {
                    System.out.println("触发：" + watchedEvent.getType() + ":路径:"+ watchedEvent.getPath()
                            + "创建后的值："
                            + zooKeeper.getData(watchedEvent.getPath(), true, stat));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else if (Event.EventType.NodeDeleted == watchedEvent.getType()) {  //
                    System.out.println("触发：" + watchedEvent.getType() + ":路径:" + watchedEvent.getPath()+ "已删除值");
            }

        }
//        System.out.println("watchedEvent state: " + watchedEvent.getState());

    }
}
