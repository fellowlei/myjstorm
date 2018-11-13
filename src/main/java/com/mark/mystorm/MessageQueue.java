package com.mark.mystorm;


import java.util.List;
import java.util.concurrent.*;

/**
 * Created by lei.lu on 18/9/13.
 */
public class MessageQueue {
    public static BlockingQueue<Integer>  blockingQueue = new ArrayBlockingQueue<Integer>(10000);
    public static ExecutorService executorServiceProducer = Executors.newSingleThreadExecutor();
    public static ExecutorService executorServiceConsumer = Executors.newSingleThreadExecutor();


    public static void init(){
        executorServiceProducer.submit(new Runnable() {
            @Override
            public void run() {
                for(int i=0; i<100;i++){
                    blockingQueue.offer(1);
                    System.out.println("insert:" + 1);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
//        executorServiceConsumer.submit(new Runnable() {
//            @Override
//            public void run() {
//                for(int i=0;i<100; i++){
//                    Integer val = null;
//                    try {
//                        val = blockingQueue.take();
//                        System.out.println("take:" + val);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//
//                }
//            }
//        });
    }

    public static void main(String[] args) {
        init();
    }
}
