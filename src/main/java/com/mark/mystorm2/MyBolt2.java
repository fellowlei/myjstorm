package com.mark.mystorm2;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lei.lu on 18/9/14.
 */
public class MyBolt2 implements IRichBolt{
    public static final Logger logger = LoggerFactory.getLogger(MyBolt2.class);
    OutputCollector outputCollector;

    public AtomicInteger sum = new AtomicInteger(0);
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Integer id = tuple.getInteger(0);
        Integer msg = tuple.getInteger(1);
        logger.error("id={},msg={},sum={}",id,msg,sum.get());
        sum.getAndAdd(msg);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id","msg","sum"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
