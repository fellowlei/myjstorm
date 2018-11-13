package com.mark.mystorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by lei.lu on 18/11/12.
 */
public class MySpout  implements IRichSpout{

    Logger logger = LoggerFactory.getLogger(MySpout.class);

    SpoutOutputCollector spoutOutputCollector;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {
        logger.info("MySpout.activate");
    }

    @Override
    public void deactivate() {
        logger.info("MySpout.deactivate");
    }

    @Override
    public void nextTuple() {
        logger.info("MySpout.nextTuple.hello");
        spoutOutputCollector.emit(new Values("hello"));
    }

    @Override
    public void ack(Object o) {
        logger.info("MySpout.ack");
    }

    @Override
    public void fail(Object o) {
        logger.info("MySpout.fail");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
