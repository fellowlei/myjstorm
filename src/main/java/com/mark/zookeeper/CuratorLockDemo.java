package com.mark.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by lei.lu on 18/11/19.
 */
public class CuratorLockDemo {

    public static void main(String[] args) throws Exception {
        zkReadWriteLockDemo();
    }

    public static void zkReadWriteLockDemo() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
        client.start();

        InterProcessReadWriteLock readWriteLock = new InterProcessReadWriteLock(client, "/read－write-lock");
        InterProcessMutex readLock = readWriteLock.readLock();
        final InterProcessMutex writeLock = readWriteLock.writeLock();

        try {
            readLock.acquire();
            System.out.println(Thread.currentThread() + "获取到读锁");

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        //在读锁没释放之前不能读取写锁。
                        writeLock.acquire();
                        System.out.println(Thread.currentThread() + "获取到写锁");
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            writeLock.release();
                            System.out.println(Thread.currentThread() + "释放写锁");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
            //停顿3000毫秒不释放锁，这时其它线程可以获取读锁，却不能获取写锁。
            Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            readLock.release();
            System.out.println(Thread.currentThread() + "释放读锁");
        }

        Thread.sleep(10000);
        client.close();
    }


    public static void zkLockDemo() throws Exception {
        //操作失败重试机制 1000毫秒间隔 重试3次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
        client.start();

        /**
         * 这个类是线程安全的，一个JVM创建一个就好
         * mylock 为锁的根目录，我们可以针对不同的业务创建不同的根目录
         */
        final InterProcessMutex lock = new InterProcessMutex(client, "/mylock");
        try {
            //阻塞方法，获取不到锁线程会挂起。
            lock.acquire();
            System.out.println("已经获取到锁");
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.release();
        }

        Thread.sleep(10000);

        client.close();
    }
}
