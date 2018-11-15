package com.mark.mystorm3;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mark.kafka.MyProducer;
import com.mark.mystorm3.redis.JedisUtil;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by lei.lu on 18/11/13.
 */
public class MyBolt implements IRichBolt {
    public static final Logger logger = LoggerFactory.getLogger(MySpout.class);

    OutputCollector outputCollector;
    Producer<String, String> producer;
    Jedis jedis = null;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        producer= MyProducer.getProducer();
        jedis = JedisUtil.getJedis();
    }

    @Override
    public void execute(Tuple tuple) {
        String name = tuple.getString(0);
        name =  name + "::bolt";
        producer.send(new ProducerRecord<String, String>("queue2",name));
//        jedis.rpush("queue2",name + "::bolt");
        logger.info("bolt received: " + name + ", send to kafka: " + name);
    }

    @Override
    public void cleanup() {

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
