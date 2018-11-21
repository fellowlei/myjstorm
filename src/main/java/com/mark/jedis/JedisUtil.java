package com.mark.jedis;

import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lei.lu on 18/11/14.
 */
public class JedisUtil {


    public static Jedis getJedis() {
        Jedis jedis = new Jedis("localhost", 6379);
//        String result = jedis.set("name", "mark");
//        System.out.println(result);
//        result = jedis.get("name");
//        System.out.println(result);
        return jedis;
    }

    public static JedisPool getJedisPool() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(10);
        jedisPoolConfig.setMaxTotal(30);
        jedisPoolConfig.setMinIdle(10);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, "localhost", 6379);
//        Jedis jedis = jedisPool.getResource();
//        String result = jedis.set("name", "mark2");
//        System.out.println(result);
//        result  = jedis.get("name");
//        System.out.println(result);
//        jedis.close();
        return jedisPool;
    }

    public static void main(String[] args) {
//        Jedis jedis = getJedis();
//        for(int i=0; i<10; i++){
//            jedis.rpush("queue","name" + i);
//        }
//
//        for(int i=0; i<10; i++){
//            String result = jedis.lpop("queue");
//            System.out.println(result);
//        }
//        getJedisShardPool();
        getDirectJedisShardPool();
    }

    public static void getJedisShardPool() {
        // 分片
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        JedisShardInfo jedisShardInfo = new JedisShardInfo("localhost", 6379);
        shards.add(jedisShardInfo);
        jedisShardInfo = new JedisShardInfo("localhost", 6379);
        shards.add(jedisShardInfo);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        ShardedJedisPool pool = new ShardedJedisPool(jedisPoolConfig, shards);

        ShardedJedis jedis = pool.getResource();
        jedis.set("a", "foo");
        String result = jedis.get("a");
        System.out.println(result);
        jedis.close();

        ShardedJedis jedis2 = pool.getResource();
        jedis2.set("z", "bar");
        result = jedis2.get("z");
        System.out.println(result);
        jedis2.close();


//        pool.close();


    }

    public static void getDirectJedisShardPool() {
        // 分片
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        JedisShardInfo jedisShardInfo = new JedisShardInfo("localhost", 6379);
        shards.add(jedisShardInfo);
        jedisShardInfo = new JedisShardInfo("localhost", 6379);
        shards.add(jedisShardInfo);

        ShardedJedis jedis = new ShardedJedis(shards);
        jedis.set("a", "foo");
        String result = jedis.get("a");
        System.out.println(result);
        jedis.close();
    }
}
