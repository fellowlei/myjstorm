package com.mark.mystorm3.redis;

import redis.clients.jedis.Jedis;

/**
 * Created by lei.lu on 18/11/15.
 */
public class JedisProducer {

    public static void main(String[] args) throws InterruptedException {
        Jedis jedis = JedisUtil.getJedis();
        for(int i=0;i<10000; i++){
            String msg = "qmsg::"+i;
            System.out.println("send: " + msg);
            jedis.rpush("queue",msg);
            Thread.sleep(100);
        }
    }
}
