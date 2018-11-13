package com.mark.mystorm3.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by lei.lu on 18/11/12.
 */
public class MyProducer {
    public static Lock lock = new ReentrantLock();
    public static Producer<String,String> producer = null;
    static {
        producer = init();
    }

    private static Producer<String, String> init(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }

    public static Producer<String, String> getProducer(){
        if(producer == null){
            lock.lock();
            try{
                producer = init();
            }finally {
                lock.unlock();
            }
        }
        return producer;
    }

    public static void main(String[] args) {
        Producer<String, String> producer = getProducer();
        for(int i=0;i<100; i++){
            producer.send(new ProducerRecord<String, String>("input","hello"));
            System.out.println("send hello");
        }

    }
}
