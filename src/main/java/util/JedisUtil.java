package util;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/14 13:51
 * @Function:
 */
public class JedisUtil {

    private static final JedisPool jedisPool;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);

        jedisPool = new JedisPool(poolConfig,"hadoop102",6379,10000);
    }

    public static StatefulRedisConnection<String, String> getAsyncRedisConnection() {
        // Get async connection to redis
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379");
        return redisClient.connect();
    }

    public static Jedis getJedis(){
        //System.out.println("~~~获取Jedis客户端~~~");
        return jedisPool.getResource();
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
    }
}

