package com.mark.mystorm3.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by lei.lu on 18/11/14.
 */
public class JedisUtil {


    public static Jedis getJedis(){
        Jedis jedis = new Jedis("localhost",6379);
        return jedis;
    }

    public static JedisPool getJedisPool(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(10);
        jedisPoolConfig.setMaxTotal(30);
        jedisPoolConfig.setMinIdle(10);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig,"localhost",6379);
        return jedisPool;
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        for(int i=0; i<10; i++){
            jedis.rpush("queue","name" + i);
        }

        for(int i=0; i<10; i++){
            String result = jedis.lpop("queue");
            System.out.println(result);
        }

    }


}
