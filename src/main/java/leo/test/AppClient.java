package leo.test;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.Validate;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by liangkyzy on 2017/6/4.
 */
public class AppClient {
    static final Logger logger = LoggerFactory.getLogger(AppClient.class);

    static final String CONNECTION_STRING = "server1:2181,server2:2181,server3:2181";
    static final Integer SESSION_TIMEOUT = 2000;
    static final String GROUP_NODE = "/servers";

    private volatile List<String> serverList = null;

    private ZooKeeper zooKeeper = null;

    //创建zk连接，并注册监听
    private void connectZk() throws IOException {
        this.zooKeeper = new ZooKeeper(CONNECTION_STRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    makeServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //获取服务器连接列表
    private void makeServerList() throws KeeperException, InterruptedException {
        Validate.notNull(this.zooKeeper);

        //core line
        List<String> children = this.zooKeeper.getChildren(GROUP_NODE, true);
        List<String> servers = Lists.newArrayList();

        for (String child : children) {
            byte[] data = zooKeeper.getData(GROUP_NODE + "/" + child, null, null);
            servers.add(new String(data));
        }
        this.serverList = servers;

        //for debug
        System.out.println(this.serverList);
    }

    public void handleBussiness() throws InterruptedException {
        logger.info("客户端开始处理业务");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        AppClient client = new AppClient();
        client.connectZk();
        client.makeServerList();
        client.handleBussiness();
    }
}
