package com.mark.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by lei.lu on 18/11/12.
 */
public class MyReceiver {
    public static Lock lock = new ReentrantLock();

    public static KafkaConsumer<String, String> consumer = null;
    static {
        consumer = init();
    }

    public static synchronized KafkaConsumer<String, String> init(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        return consumer;
    }

    public static KafkaConsumer<String,String> getConsumer(){
        if(consumer == null){
            lock.lock();
            try{
                consumer = init();
            }finally {
                lock.unlock();
            }
        }
        return consumer;
    }
}
