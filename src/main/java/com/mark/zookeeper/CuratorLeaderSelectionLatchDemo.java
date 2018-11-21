package com.mark.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lei.lu on 18/11/19.
 */
public class CuratorLeaderSelectionLatchDemo {

    public static void main(String[] args) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            final int index = i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        new CuratorLeaderSelectionLatchDemo().schedule(index);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        //休眠50秒之后结束main方法
        Thread.sleep(30 * 1000);
        service.shutdownNow();
    }



    private void schedule(final int thread) throws Exception {
        CuratorFramework client = getClient(thread);
        LeaderLatch latch = new LeaderLatch(client, "/myleader", String.valueOf(thread));

        //给latch添加监听，在
        latch.addListener(new LeaderLatchListener() {

            @Override
            public void notLeader() {
                //如果不是leader
                System.out.println("Client [" + thread + "] I am the follower !");
            }

            @Override
            public void isLeader() {
                //如果是leader
                System.out.println("Client [" + thread + "] I am the leader !");
            }
        });

        //开始选取 leader
        latch.start();

        Thread.sleep(10000);
        if (latch != null) {
            //CloseMode.NOTIFY_LEADER 节点状态改变时,通知LeaderLatchListener
            latch.close(LeaderLatch.CloseMode.NOTIFY_LEADER);
        }
        if (client != null) {
            client.close();
        }
        System.out.println("Client [" + latch.getId() + "] Server closed...");
    }

    private CuratorFramework getClient(final int thread) {
        RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);
        // Fluent风格创建
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .sessionTimeoutMs(1000000)
                .connectionTimeoutMs(3000)
                .retryPolicy(rp)
                .build();
        client.start();
        System.out.println("Client [" + thread + "] Server connected...");
        return client;
    }

}
