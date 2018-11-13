package com.mark.mystorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lei.lu on 18/11/12.
 */
public class MyTopology {

    public static String SPOUT_NAME = "MY_SPOUT";
    public static String BOLT_NAME = "MY_BOLT";
    public static String TOPOLOGY_NAME = "MY_TOPOLOGY";

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {


        TopologyBuilder builder = new TopologyBuilder();


        SpoutDeclarer spout = builder.setSpout(SPOUT_NAME,
                new MySpout(), 1);


        BoltDeclarer totalBolt = builder.setBolt(BOLT_NAME, new MyBolt(),
                1).shuffleGrouping(SPOUT_NAME);


        Map conf = new HashMap();
        Config.setNumAckers(conf, 1);
        conf.put(Config.TOPOLOGY_WORKERS, 1);

        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology(TOPOLOGY_NAME, conf,
                builder.createTopology());


        Thread.sleep(60000);

        localCluster.killTopology("SequenceTest");
        localCluster.shutdown();
    }
}
