package com.mark.mystorm2;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lei.lu on 18/9/14.
 */
public class MySpout implements IRichSpout{
    public static final Logger logger = LoggerFactory.getLogger(MySpout.class);
    SpoutOutputCollector collector;
    AtomicInteger idGen = new AtomicInteger();
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
        logger.info("init");
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
        int id = idGen.incrementAndGet();
        int msg = 1;
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.error("spout id={},msg={}",id,msg);
        collector.emit(new Values(id,msg));
    }

    @Override
    public void ack(Object o) {
        logger.error("success ack");
    }

    @Override
    public void fail(Object o) {
        logger.error("failed ack");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id","msg"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
