package com.mark.mystorm3.redis;

import redis.clients.jedis.Jedis;
import shade.storm.org.apache.commons.lang.StringUtils;

/**
 * Created by lei.lu on 18/11/15.
 */
public class JedisConsumer {

    public static void main(String[] args) throws InterruptedException {
        Jedis jedis = JedisUtil.getJedis();
       while (true){
           String val = jedis.lpop("queue2");
           if(StringUtils.isEmpty(val)){
               Thread.sleep(100);
               continue;
           }
           System.out.println("received: " + val);
       }
    }
}
