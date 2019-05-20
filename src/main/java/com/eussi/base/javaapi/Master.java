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

    static boolean isLeader;

    enum MasterStates {RUNNING, ELECTED, NOTELECTED};
    private volatile MasterStates state = MasterStates.RUNNING;

    private volatile boolean connected = false;
    private volatile boolean expired = false;

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
    public void process(WatchedEvent e) {
        LOG.info("Processing event: " + e.toString());
        if(e.getType() == Event.EventType.None){
            switch (e.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("Session expiration");
                default:
                    break;
            }
        }
    }

    boolean isConnected() {
        return connected;
    }

    boolean isExpired() {
        return expired;
    }

    void stopZK() throws InterruptedException, IOException {
        zk.close();
    }

    public static void main(String args[]) throws Exception {
        Master m = new Master("192.168.198.202:2181");
        m.startZK();

        while(!m.isConnected()){
            Thread.sleep(100);
        }

        m.bootstrap();

        m.runForMaster();

        while(!m.isExpired()){
            Thread.sleep(1000);
        }

        m.stopZK();

//        if (m.isLeader) { //异步方法，此处判断时，可能还未回调
//            System.out.println("I'm the leader");
//            // wait for a bit
////            Thread.sleep(60000);
//        } else {
//            System.out.println("Someone else is the leader");
//        }

    }

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I'm " + (isLeader ? "" : "not ") +
                    "the leader");
        }
    };
    void runForMaster() {
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

//    public void runForMaster() throws InterruptedException {
//        LOG.info("Running for master");
//        while (true) {
//            try {
//                zk.create("/master",
//                        serverId.getBytes(),
//                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
//                        CreateMode.EPHEMERAL);
//                isLeader = true;
//                break;
//            } catch (KeeperException.NodeExistsException e) {
//                isLeader = false;
//                break;
//            } catch (KeeperException.ConnectionLossException e) {
//                //ConnectionLossException 子异常，发生于客户端与ZooKeeper服务端失去连接时。
//                // 一般常常由于网络原因导致，如网络分区或ZooKeeper服务器故障
//                //当这个异常发生时，客户端并不知道是在ZooKeeper服务器处理前丢失了请求消息，还是在处理后客户端未收到响应消息
//
//            } catch (KeeperException e) {
//
//            }
//
//            if (checkMaster()) break;
//        }
//    }

    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }



    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;

                case NONODE:
                    runForMaster();
                    return;
            }
        }
    };


//    boolean checkMaster() throws InterruptedException {
//        while (true) {
//            try {
//                Stat stat = new Stat();
//                byte data[] = zk.getData("/master", false, stat);
//                isLeader = new String(data).equals(serverId);
//                return true;
//            } catch (KeeperException.NoNodeException e) {
//                // no master, so try create again
//                return false;
//            } catch (KeeperException.ConnectionLossException e) {
//
//            } catch (KeeperException e) {
//
//            }
//        }
//    }

    public void bootstrap(){
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data){
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentCallback,
                data);
    }

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                /*
                 * Try again. Note that registering again is not a problem.
                 * If the znode has already been created, then we get a
                 * NODEEXISTS event back.
                 */
                    createParent(path, (byte[]) ctx);

                    break;
                case OK:
                    LOG.info("Parent created");

                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already registered: " + path);

                    break;
                default:
                    LOG.error("Something went wrong: ",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


}
