package com.eussi.base.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;

/**
 * Created by wangxueming on 2018/9/30.
 */
public class CuratorEventDemo {
    /**
     * 三种watcher来做时间监听：
     * PathCache 监视一个路径下子节点的创建，更新，删除
     * NodeCache 监视一个节点的创建，更新，删除
     * TreeCache 以上两个综合起来，监视路径下的创建，删除，更新，缓存路径下所有子节点的数据
     */



     public static void main(String[] args) throws IOException, InterruptedException {
        CuratorFramework curatorFramework = CuratorClientUtils.getInstance();

         try {
             String path = "/cache";
             final NodeCache nodeCache = new NodeCache(curatorFramework, path, false);
             nodeCache.start(true);

             nodeCache.getListenable().addListener(new NodeCacheListener() {
                 @Override
                 public void nodeChanged() throws Exception {
                     System.out.println("节点数据发生了变化");
                 }
             });

             curatorFramework.create().forPath(path, "123".getBytes());
             System.out.println("创建节点");

             curatorFramework.setData().forPath(path, "123456".getBytes());
             System.out.println("更新节点");

             curatorFramework.delete().forPath(path);
             System.out.println("删除节点");
             Thread.sleep(2000);

             System.out.println("--------------------------------------------------------------------------------");

             PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, path, true);
             pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

             pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                 @Override
                 public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                     switch (pathChildrenCacheEvent.getType()) {
                         case CHILD_ADDED:
                             System.out.println(pathChildrenCacheEvent.getType() + ":增加子节点");
                             break;
                         case CHILD_REMOVED:
                             System.out.println(pathChildrenCacheEvent.getType() + ":删除子节点");
                             break;
                         case CHILD_UPDATED:
                             System.out.println(pathChildrenCacheEvent.getType() + ":更新子节点");
                             break;
                         default:break;
                     }
                 }
             });

             curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(path, "123".getBytes());
             System.out.println("创建节点");
             Thread.sleep(2000);      //触发太快会造成监听不到的情况

             curatorFramework.create().forPath(path + "/child", "123".getBytes());
             System.out.println("创建子节点");
             Thread.sleep(2000);

             curatorFramework.setData().forPath(path + "/child", "123456".getBytes());
             System.out.println("更新子节点");
             Thread.sleep(2000);

             curatorFramework.delete().forPath(path + "/child");
             System.out.println("删除子节点");
             Thread.sleep(2000);

             curatorFramework.delete().forPath(path);
             System.out.println("删除节点");

             Thread.sleep(5000);

             System.in.read();

         } catch (Exception e) {
             e.printStackTrace();
         }

     }
}
