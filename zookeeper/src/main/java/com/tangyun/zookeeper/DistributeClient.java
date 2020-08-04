package com.tangyun.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author YunTang
 * @create 2020-06-21 10:42
 */
public class DistributeClient {


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeClient client = new DistributeClient();

        // 1获取zookeeper连接
        client.getConnect();

        // 2注册监听
        client.getChlidren();

        //3 业务逻辑处理
        client.business();
    }

    private void business() throws InterruptedException {

        Thread.sleep(Long.MAX_VALUE);
    }

    private void getChlidren() throws KeeperException, InterruptedException {

        List<String> children = zkClient.getChildren("/servers", true);

        // 存储服务器结点主机名称
        ArrayList<String> hosts = new ArrayList();

        for (String child : children) {

            byte[] data = zkClient.getData("/servers/" + child, false, null);
            hosts.add(new String(data));

        }

        // 将所有在线主机名称打印到控制台
        System.out.println(hosts);
    }

    private String connectString = "master:2181,slave1:2181,slave2:2181";
    private int sessionTimeout = 20000;
    private ZooKeeper zkClient;

    private void getConnect() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

                try {
                    getChlidren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });
    }
}
