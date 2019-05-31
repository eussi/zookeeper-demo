package com.eussi.base.javaapi.master_slave;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.eussi.base.javaapi.master_slave.recovery.RecoveredAssignments;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;

import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Master implements Watcher, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    
    /*
     * A master process can be either running for
     * primary master, elected primary master, or
     * not elected, in which case it is a backup
     * master.  
     */
    enum MasterStates {RUNNING, ELECTED, NOTELECTED};

    private volatile MasterStates state = MasterStates.RUNNING;

    /**
     * Main method providing an example of how to run the master.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        Master m = new Master("192.168.198.202:2181");

        m.startZK();//实例化zookeeper

        while(!m.isConnected()){//实例化zookeeper过程中会建立连接，这里一直等到连接建立成功，结束循环
            Thread.sleep(100);
        }

        m.bootstrap();//创建一些主要节点

        /*
         * now runs for master.
         */
        m.runForMaster();//创建master节点

        while(!m.isExpired()){
            Thread.sleep(1000);
        }

        m.stopZK();
    }

    MasterStates getState() {
        return state;
    }
    
    private Random random = new Random(this.hashCode());
    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString(random.nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    
    protected ChildrenCache tasksCache;
    protected ChildrenCache workersCache;
    
    /**
     * Creates a new master instance.
     * 
     * @param hostPort
     */
    Master(String hostPort) {
        /*
            在构造函数中，我们并未实例化ZooKeeper对象，而是先保存
            hostPort留着后面使用。Java最佳实践告诉我们，一个对象的构造函数没
            有完成前不要调用这个对象的其他方法。
            因为这个对象实现了Watcher，并且当我们实例化ZooKeeper对象时，其Watcher的回调函数就
            会被调用，所以我们需要Master的构造函数返回后再调用ZooKeeper的构造函数。
         */
        this.hostPort = hostPort;
    }

    /**
     * Check if this client is connected.
     *
     * @return boolean ZooKeeper client is connected
     */
    boolean isConnected() {
        return connected;
    }

    /**
     * Check if the ZooKeeper session has expired.
     *
     * @return boolean ZooKeeper session has expired
     */
    boolean isExpired() {
        return expired;
    }
    
    
    /**
     * Creates a new ZooKeeper session.
     * 
     * @throws java.io.IOException
     */
    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);//传入watcher对象
    }

    /**
     * Closes the ZooKeeper session.
     *
     * @throws java.io.IOException
     */
    void stopZK() throws InterruptedException, IOException {
        zk.close();
    }

    /**
     * This method implements the process method of the
     * Watcher interface. We use it to deal with the
     * different states of a session.
     *
     * @param e new session event to be processed
     */
    public void process(WatchedEvent e) {
        LOG.info("Processing event: " + e.toString());
        /*
        WatchedEvent数据结构包括以下信息：
            ·ZooKeeper会话状态（KeeperState）：Disconnected、
                SyncConnected、AuthFailed、ConnectedReadOnly、SaslAuthenticated和
                Expired。
            ·事件类型（EventType）：NodeCreated、NodeDeleted、
                NodeDataChanged、NodeChildrenChanged和None。
            ·如果事件类型不是None时，返回一个znode路径。
         */
        if(e.getType() == EventType.None){
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


    /**
     * This method creates some parent znodes we need for this example.
     * In the case the master is restarted, then this method does not
     * need to be executed a second time.
     */
    public void bootstrap(){
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data){
        zk.create(path,
                data,
                Ids.OPEN_ACL_UNSAFE,//为所有人提供了所有权限（正如其名所显示的，这个ACL策略在不可信的环境下使用是非常不安全的）
                CreateMode.PERSISTENT,//持久节点
                createParentCallback,//异步回调
                data);//callback中的ctx，上下文参数
    }

    StringCallback createParentCallback = new StringCallback() {
        public void processResult(int rc, //返回调用的结构，返回OK或与KeeperException异常对应的编码值。
                                  String path, //传给create的path参数值
                                  Object ctx, //传给create的上下文参数
                                  String name) {//创建的znode节点名称
            switch (Code.get(rc)) {
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
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };


    /*
     * Run for master. To run for master, we try to create the /master znode,
     * with masteCreateCallback being the callback implementation.
     * In the case the create call succeeds, the client becomes the master.
     * If it receives a CONNECTIONLOSS event, then it needs to check if the
     * znode has been created. In the case the znode exists, it needs to check
     * which server is the master.
     */

    /**
     * Tries to create a /master lock znode to acquire leadership.
     */
    public void runForMaster() {
        LOG.info("Running for master");
        zk.create("/master",
                serverId.getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,//创建临时节点
                masterCreateCallback,
                null);
    }


    /*
     * The story in this callback implementation is the following.
     * We tried to create the master lock znode. If it suceeds, then
     * great, it takes leadership. However, there are a couple of
     * exceptional situations we need to take care of.
     *
     * First, we could get a connection loss event before getting
     * an answer so we are left wondering if the operation went through.
     * To check, we try to read the /master znode. If it is there, then
     * we check if this master is the primary. If not, we run for master
     * again.
     *
     *  The second case is if we find that the node is already there.
     *  In this case, we call exists to set a watch on the znode.
     */
    StringCallback masterCreateCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS://连接丢失，需要检测是否主节点已经创建
                    checkMaster();
                    break;
                case OK://创建成功，则证明选举成功
                    state = MasterStates.ELECTED;
                    takeLeadership();//进行主节点分配操作

                    break;
                case NODEEXISTS://已经存在，则证明已经选举了主节点
                    state = MasterStates.NOTELECTED;
                    masterExists();

                    break;
                default:
                    state = MasterStates.NOTELECTED;
                    LOG.error("Something went wrong when running for master.",
                            KeeperException.create(Code.get(rc), path));
            }
            LOG.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
        }
    };


    void checkMaster() {
        zk.getData("/master",
                false, //是否设置监视点
                masterCheckCallback,
                null);
    }

    DataCallback masterCheckCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS://连接丢失，继续检测
                    checkMaster();
                    break;
                case NONODE://不存在，则继续创建主节点
                    runForMaster();
                    break;
                case OK://存在主节点，检测是否是自己的，否则不是自己的
                    if( serverId.equals( new String(data) ) ) {
                        state = MasterStates.ELECTED;
                        takeLeadership();
                    } else {
                        state = MasterStates.NOTELECTED;
                        masterExists();
                    }

                    break;
                default:
                    LOG.error("Error when reading data.",
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void masterExists() {//检测是否已经存在
        zk.exists("/master",
                masterExistsWatcher,//监视master事件
                masterExistsCallback,
                null);
    }

    Watcher masterExistsWatcher = new Watcher(){
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeDeleted) {
                assert "/master".equals( e.getPath() );

                runForMaster();//主节点删除事件在此创建主节点
            }
        }
    };


    StatCallback masterExistsCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat){
            switch (Code.get(rc)) {
                case CONNECTIONLOSS://连接丢失，继续检测是否存在
                    masterExists();
                    break;
                case OK:
                    break;
                case NONODE://不存在，继续创建主节点
                    state = MasterStates.RUNNING;
                    runForMaster();
                    LOG.info("It sounds like the previous master is gone, " +
                            "so let's run for master again.");

                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };


    /**
     * 成功成为主节点，执行主节点操作
     */
    void takeLeadership() {
        LOG.info("Going for list of workers");
        getWorkers();//获取woker节点列表

        (new RecoveredAssignments(zk)).recover( new RecoveredAssignments.RecoveryCallback() {
            public void recoveryComplete(int rc, List<String> tasks) {
                if(rc == RecoveredAssignments.RecoveryCallback.FAILED) {
                    LOG.error("Recovery of assigned tasks failed.");
                } else {
                    LOG.info( "Assigning recovered tasks" );
                    getTasks();
                }
            }
        });
    }

    void getWorkers(){
        zk.getChildren("/workers",
                workersChangeWatcher,
                workersGetChildrenCallback,
                null);
    }

    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {//worker子节点变动
                assert "/workers".equals( e.getPath() );

                getWorkers();
            }
        }
    };

    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK:
                    LOG.info("Succesfully got a list of workers: "
                            + children.size()
                            + " workers");
                    reassignAndSet(children);//成功获取后，分配任务
                    break;
                default:
                    LOG.error("getChildren failed",
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

        /*
     *******************
     *******************
     * Assigning tasks.*
     *******************
     *******************
     */

    void reassignAndSet(List<String> children){
        List<String> toProcess;

        if(workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            LOG.info( "Removing and setting" );
            toProcess = workersCache.removedAndSet( children );//获取新数组中比原数组少的数据
        }

        if(toProcess != null) {
            for(String worker : toProcess){
                getAbsentWorkerTasks(worker);
            }
        }
    }


    /*
     ****************************************************
     ****************************************************
     * Methods to handle changes to the list of workers.*
     ****************************************************
     ****************************************************
     */


    /**
     * This method is here for testing purposes.
     *
     * @return size Size of the worker list
     */
    public int getWorkersSize(){
        if(workersCache == null) {
            return 0;
        } else {
            return workersCache.getList().size();
        }
    }

    void getAbsentWorkerTasks(String worker){
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }

    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                getAbsentWorkerTasks(path);

                break;
            case OK://这个worker已经不再了，但是还有正在执行的任务，将其任务重新分配
                LOG.info("Succesfully got a list of assignments: "
                        + children.size()
                        + " tasks");

                /*
                 * Reassign the tasks of the absent worker.
                 */

                for(String task: children) {//重新分配任务
                    getDataReassign(path + "/" + task, task);
                }
                break;
            default:
                LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. *
     ************************************************
     */

    /**
     * Get reassigned task data.
     *
     * @param path Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String path, String task) {
        zk.getData(path,
                false,
                getDataReassignCallback,
                task);
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, (String) ctx);

                break;
            case OK:
                recreateTask(new RecreateTaskCtx(path, (String) ctx, data));//重新创建任务

                break;
            default:
                LOG.error("Something went wrong when getting data ",
                        KeeperException.create(Code.get(rc)));
            }
        }
    };

    /**
     * Recreate task znode in /tasks
     *
     * @param ctx Recreate text context
     */
    void recreateTask(RecreateTaskCtx ctx) {
        zk.create("/tasks/" + ctx.task,
                ctx.data,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                recreateTaskCallback,
                ctx);
    }

    /**
     * Recreate znode callback
     */
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((RecreateTaskCtx) ctx);

                break;
            case OK:
                deleteAssignment(((RecreateTaskCtx) ctx).path);

                break;
            case NODEEXISTS:
                LOG.info("Node exists already, but if it hasn't been deleted, " +
                		"then it will eventually, so we keep trying: " + path);
                recreateTask((RecreateTaskCtx) ctx);

                break;
            default:
                LOG.error("Something wwnt wrong when recreating task",
                        KeeperException.create(Code.get(rc)));
            }
        }
    };

    void deleteAssignment(String path){
        zk.delete(path, -1, taskDeletionCallback, null);
    }

    /**
     * Delete assignment of absent worker
     *
     * @param path Path of znode to be deleted
     */


    VoidCallback taskDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteAssignment(path);
                    break;
                case OK:
                    LOG.info("Task correctly deleted: " + path);
                    break;
                default:
                    LOG.error("Failed to delete task data" +
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /**
     * Context for recreate operation.
     *
     */
    class RecreateTaskCtx {
        String path;
        String task;
        byte[] data;

        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /*
     ******************************************************
     ******************************************************
     * Methods for receiving new tasks and assigning them.*
     ******************************************************
     ******************************************************
     */

    Watcher tasksChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert "/tasks".equals( e.getPath() );

                getTasks();
            }
        }
    };

    void getTasks(){
        zk.getChildren("/tasks",
                tasksChangeWatcher,
                tasksGetChildrenCallback,
                null);
    }

    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();

                break;
            case OK:
                List<String> toProcess;
                if(tasksCache == null) {
                    tasksCache = new ChildrenCache(children);

                    toProcess = children;
                } else {
                    toProcess = tasksCache.addedAndSet( children );
                }

                if(toProcess != null){
                    assignTasks(toProcess);
                }

                break;
            default:
                LOG.error("getChildren failed.",
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void assignTasks(List<String> tasks) {
        for(String task : tasks){
            getTaskData(task);
        }
    }

    void getTaskData(String task) {
        zk.getData("/tasks/" + task,
                false,
                taskDataCallback,
                task);
    }

    DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getTaskData((String) ctx);

                break;
            case OK:
                /*
                 * Choose worker at random.
                 */
                List<String> list = workersCache.getList();
                String designatedWorker = list.get(random.nextInt(list.size()));

                /*
                 * Assign task to randomly chosen worker.
                 */
                String assignmentPath = "/assign/" +
                        designatedWorker +
                        "/" +
                        (String) ctx;
                LOG.info( "Assignment path: " + assignmentPath );
                createAssignment(assignmentPath, data);

                break;
            default:
                LOG.error("Error when trying to get task data.",
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void createAssignment(String path, byte[] data){
        zk.create(path,
                data,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                assignTaskCallback,
                data);
    }

    StringCallback assignTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                createAssignment(path, (byte[]) ctx);

                break;
            case OK:
                LOG.info("Task assigned correctly: " + name);
                deleteTask(name.substring( name.lastIndexOf("/") + 1));

                break;
            case NODEEXISTS:
                LOG.warn("Task already assigned");

                break;
            default:
                LOG.error("Error when trying to assign task.",
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /*
     * Once assigned, we delete the task from /tasks
     */
    void deleteTask(String name){
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
    }

    VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteTask(path);

                break;
            case OK:
                LOG.info("Successfully deleted " + path);

                break;
            case NONODE:
                LOG.info("Task has been deleted already");

                break;
            default:
                LOG.error("Something went wrong here, " +
                        KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /**
     * Closes the ZooKeeper session.
     *
     * @throws java.io.IOException
     */
    @Override
    public void close() throws IOException {
        if(zk != null) {
            try{
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn( "Interrupted while closing ZooKeeper session.", e );
            }
        }
    }
    
}
