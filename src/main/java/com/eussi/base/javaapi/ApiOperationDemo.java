package com.eussi.base.javaapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * Created by wangxueming on 2018/9/29.
 */
public class ApiOperationDemo implements Watcher{
    public final static String CONNECTIONSTRING =  "192.168.198.201:2181," +
            "192.168.198.202:2181," +
            "192.168.198.203:2181," +
            "192.168.198.204:2181";
    private static ZooKeeper zooKeeper = null;
    private static Stat stat = new Stat();
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        zooKeeper = new ZooKeeper(CONNECTIONSTRING, 5000, new ApiOperationDemo());
        System.out.println("zooKeeper state: " + zooKeeper.getState());
        Thread.sleep(2000);

        String path = "/java";
        Stat stat = zooKeeper.exists(path, true);//继续监视，exits和getData设置数据监视，而getChildren设置子节点监视
        if(stat==null) {
            //创建节点
            String result = zooKeeper.create(path, "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL); //触发NodeCreated，临时节点下无法新增节点
            System.out.println("创建" + path + "成功：" + result);
            Thread.sleep(2000);
        }

        //修改节点
        zooKeeper.setData(path, "456".getBytes(), -1); //触发NodeDataChanged
        System.out.println("修改 " + path + " 成功！！！");
        Thread.sleep(2000);

        System.out.println("--------------------------------------------------------------------------");

        path = "/JAVA";
        zooKeeper.exists(path, true);//触发NodeCreated
        String result = zooKeeper.create(path, "456".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建 " + path + " 成功：" + result);

        zooKeeper.getChildren(path, true);//为子节点设置监听

        stat = zooKeeper.exists(path + "/java22", true);//继续监视，exits和getData设置数据监视，而getChildren设置子节点监视
        if(stat==null) {
            zooKeeper.create(path + "/java22", "789".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);//触发NodeCreated、NodeChildrenChanged
            System.out.println("创建 " + path + "/java22" + " 成功：" + result);
            Thread.sleep(2000);
        }

        zooKeeper.setData(path + "/java22", "666".getBytes(), -1);//触发NodeDataChanged
        System.out.println("修改 " + path + "/java22" + " 成功！！！");
        Thread.sleep(2000);

        zooKeeper.delete(path + "/java22", -1);//触发NodeCreated、NodeChildrenChanged
        System.out.println("删除 " + path + "/java22" + " 成功！！！");
        Thread.sleep(2000);

        zooKeeper.delete(path, -1); //触发NodeDeleted
        System.out.println("删除 " + path + " 成功！！！");
        Thread.sleep(2000);

    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            if(Event.EventType.None==watchedEvent.getType() && null == watchedEvent.getPath()) {
                System.out.println(watchedEvent.getType() + ":" + watchedEvent.getState() + "-->" + watchedEvent.getType());

            } else if (Event.EventType.NodeDataChanged == watchedEvent.getType()) { //节点数据发生变化的时候触发
                try {
                    System.out.println(watchedEvent.getType() + ":路径:" + watchedEvent.getPath()
                            + "改变后的值："
                            + zooKeeper.getData(watchedEvent.getPath(), true, stat));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (Event.EventType.NodeChildrenChanged == watchedEvent.getType()) { //子节点的数据变化的时候触发
                try {
                    System.out.println(watchedEvent.getType() + ":路径:" + watchedEvent.getPath()
                            + "子节点创建后的值："
                            + zooKeeper.getData(watchedEvent.getPath(), true, stat));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (Event.EventType.NodeCreated == watchedEvent.getType()) {  //创建子节点的时候触发
                try {
                    System.out.println(watchedEvent.getType() + ":路径:"+ watchedEvent.getPath()
                            + "创建后的值："
                            + zooKeeper.getData(watchedEvent.getPath(), true, stat));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else if (Event.EventType.NodeDeleted == watchedEvent.getType()) {  //创建子节点的时候触发
                    System.out.println(watchedEvent.getType() + ":路径:" + watchedEvent.getPath()+ "已删除值");
            }

        }
//        System.out.println("watchedEvent state: " + watchedEvent.getState());

    }
}
