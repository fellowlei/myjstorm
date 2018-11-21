package com.mark.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lei.lu on 18/11/19.
 */
public class CuratorLeaderSelectionDemo {
    public static void main(String[] args) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(3);

        for (int i = 0; i < 3; i++) {
            final int index = i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        new CuratorLeaderSelectionDemo().schedule(index);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        Thread.sleep(30 * 1000);
        service.shutdownNow();
    }

    private void schedule(final int thread) throws Exception {
        CuratorFramework client = this.getClient(thread);
        MyLeaderSelectorListenerAdapter leaderSelectorListener =
                new MyLeaderSelectorListenerAdapter(client, "/myleader", "Client #" + thread);
        leaderSelectorListener.start();
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

    public static class MyLeaderSelectorListenerAdapter extends LeaderSelectorListenerAdapter implements Closeable {

        private String name;
        private LeaderSelector leaderSelector;
        public AtomicInteger leaderCount = new AtomicInteger(1);

        public MyLeaderSelectorListenerAdapter(CuratorFramework client, String path, String name) {
            this.name = name;
            this.leaderSelector = new LeaderSelector(client, path, this);

            /**
             * 自动重新排队
             * 该方法的调用可以确保此实例在释放领导权后还可能获得领导权
             */
            leaderSelector.autoRequeue();
        }

        @Override
        public void takeLeadership(CuratorFramework client) throws Exception {
            System.out.println(name + "成为当前leader" + " 共成为leader的次数：" + leaderCount.getAndIncrement() + "次");
            try {
                //模拟业务逻辑执行2秒
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                System.err.println(name + "已被中断");
                Thread.currentThread().interrupt();
            } finally {
                System.out.println(name + "放弃领导权");
            }
        }

        public void start() throws IOException {
            leaderSelector.start();
        }

        @Override
        public void close() throws IOException {
            leaderSelector.close();
        }
    }
}
