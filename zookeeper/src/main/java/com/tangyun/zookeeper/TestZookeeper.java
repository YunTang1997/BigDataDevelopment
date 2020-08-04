package com.tangyun.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import sun.awt.geom.AreaOp;

import java.io.IOException;
import java.security.PublicKey;
import java.util.List;

/**
 * @author YunTang
 * @create 2020-06-20 13:44
 */


public class TestZookeeper{

    private String connectString = "master:2181,slave1:2181,slave2:2181";   // 连接地址不允许有空格，否者会报错
    // 注意如果会花时间过段，会导致创建节点失败
    private int sessionTimeout = 20000;
    private ZooKeeper zkClient;

    //需要提前执行，否则下面创建结点会报错空指针
    @Before
    public void init() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            public void process(WatchedEvent watchedEvent) {

                System.out.println("--------------start-------------");
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/", true);

                    for (String child : children) {
                        System.out.println(child);
                    }
                    System.out.println("-------------end--------------");

                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // 1创建结点
   @Test
    public void creatNode() throws KeeperException, InterruptedException {

       String path = zkClient.create("/guanyu", "dashenzuishuai".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

       System.out.println(path);
   }

    //  2获取子结点并监控结点变化
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {

        List<String> children = zkClient.getChildren("/", true);

        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    // 3判断结点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {

        Stat stat = zkClient.exists("/suguo", false);

        System.out.println(stat == null?"not exist":"exist");
    }
}
