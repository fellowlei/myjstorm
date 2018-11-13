package com.mark.mystorm3;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.mark.kafka.MyProducer;
import com.mark.kafka.MyReceiver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lei.lu on 18/11/13.
 */
public class MySpout implements IRichSpout {
    public static final Logger logger = LoggerFactory.getLogger(MySpout.class);
    public static AtomicInteger idGen = new AtomicInteger(0);


    SpoutOutputCollector spoutOutputCollector;
    KafkaConsumer<String, String> consumer;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        consumer = MyReceiver.getConsumer();
        consumer.subscribe(Arrays.asList("input"));;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
//        ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
//        Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
//        while(iterator.hasNext()){
//            String msg = iterator.next().value();
//            spoutOutputCollector.emit(new Values(msg));
//            logger.info("send " + msg);
//        }
        String msg = "hello" + idGen.incrementAndGet();
        while (true){
            spoutOutputCollector.emit(new Values(msg));
            logger.info("send " + msg);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("name"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
