package leo.test;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by liangkyzy on 2017/6/4.
 */
public class AppServer {

    static final Logger logger = LoggerFactory.getLogger(AppServer.class);

    static final String CONNECTION_STRING = "server1:2181,server2:2181,server3:2181";
    static final Integer SESSION_TIMEOUT = 2000;
    static final String GROUP_NODE = "/servers";

    private ZooKeeper zooKeeper = null;

    //创建zk连接
    private void connectZk() throws IOException {
        this.zooKeeper = new ZooKeeper(CONNECTION_STRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("类型: "+watchedEvent.getType()+ " 路径："+watchedEvent.getPath());
            }
        });
    }

    /**
     * 向zk注册 短暂 序列 节点
     * @throws UnknownHostException
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void regisiterServer() throws UnknownHostException, KeeperException, InterruptedException {
        String hostName = InetAddress.getLocalHost().getHostName();
        String nodePath = GROUP_NODE + "/server";
        String createNodeContent = zooKeeper.create(nodePath, hostName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info("成功注册ZK -- hostname: {}, path:{}, content:{}",hostName,nodePath,hostName);
    }

    public void handleBussiness() throws InterruptedException {
        logger.info("服务端开始业务处理");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        AppServer server = new AppServer();
        server.connectZk();

        server.regisiterServer();
        server.handleBussiness();
    }
}
