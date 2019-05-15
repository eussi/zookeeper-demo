package com.eussi.base.javaapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Created by wangxueming on 2019/5/14.
 */
public class Master implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    ZooKeeper zk;
    String hostPort;

    boolean isLeader = false;

    private Random random = new Random(this.hashCode());
    private String serverId = Integer.toHexString( random.nextInt() );

    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() {
        try {
            zk = new ZooKeeper(hostPort, 15000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String args[]) throws Exception {
        Master m = new Master("192.168.198.202:2185");
        m.startZK();
        m.runForMaster();

        if (m.isLeader) {
            System.out.println("I'm the leader");
            // wait for a bit
//            Thread.sleep(60000);
        } else {
            System.out.println("Someone else is the leader");
        }
    }

    public void runForMaster() throws InterruptedException {
        LOG.info("Running for master");
        while (true) {
            try {
                zk.create("/master",
                        serverId.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException e) {
                //ConnectionLossException 子异常，发生于客户端与ZooKeeper服务端失去连接时。
                // 一般常常由于网络原因导致，如网络分区或ZooKeeper服务器故障
                //当这个异常发生时，客户端并不知道是在ZooKeeper服务器处理前丢失了请求消息，还是在处理后客户端未收到响应消息

            } catch (KeeperException e) {

            }

            if (checkMaster()) break;
        }


    }

    boolean checkMaster() throws InterruptedException {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e) {
                // no master, so try create again
                return false;
            } catch (KeeperException.ConnectionLossException e) {

            } catch (KeeperException e) {

            }
        }
    }


}
