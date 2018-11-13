package com.mark.mystorm2;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lei.lu on 18/9/14.
 */
public class MyToplogy {

    public static final String TOPOLOGY_NAME="MY_TOPOLOGY";
    public static final String SPOUT_NAME="SPOUT_NAME";
    public static final String BOLT_NAME="BOLT_NAME";
    public static final String BOLT_NAME_2="BOLT_NAME_2";

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Map conf = new HashMap(); //topology所有自定义的配置均放入这个Map

        TopologyBuilder builder = new TopologyBuilder(); //创建topology的生成器

        int spoutParal = 4; //获取spout的并发设置
        int boltParal = 1; //获取bolt的并发设置
        int ackerParal = 1; //设置表示acker的并发数
        int workerNum = 1; //表示整个topology将使用几个worker


        SpoutDeclarer spout = builder.setSpout(SPOUT_NAME, new MySpout(), spoutParal);

        BoltDeclarer mybolt = builder.setBolt(BOLT_NAME, new MyBolt(), boltParal).shuffleGrouping(SPOUT_NAME);

        BoltDeclarer mybolt2 = builder.setBolt(BOLT_NAME_2,new MyBolt2(),boltParal).localOrShuffleGrouping(BOLT_NAME);


        Config.setNumAckers(conf, ackerParal);

        conf.put(Config.TOPOLOGY_WORKERS, workerNum);

        conf.put(Config.STORM_CLUSTER_MODE, "distributed"); //设置topolog模式为分布式，这样topology就可以放到JStorm集群上运行

        StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology()); //提交topology
    }
}
