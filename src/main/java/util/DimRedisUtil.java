package util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.*;

import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/14 14:09
 * @Function:
 */
public class DimRedisUtil {
    public static JSONObject getDimInfoFromRedisOrHbase(Connection conn, Jedis jedis, String tableName, String pk) throws IOException {
        //Try to get dim data from redis first
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + pk;
        String redisVal = jedis.get(redisKey);
        if (redisVal != null) {
            //If we get data from redis, update TTL when read
            jedis.expire(redisKey, Common.REDIS_EXPIRE_SECONDS);
            return JSONObject.parseObject(redisVal);
        }
        //If we can't get from redis, get from hbase, and put it into redis
        JSONObject jsonData = HBaseUtil.getJsonData(conn, Common.HBASE_NAMESPACE, tableName, pk);
        jedis.setex(redisKey, Common.REDIS_EXPIRE_SECONDS, jsonData.toJSONString());
        return jsonData;
    }


    public static void deleteDimInfoFromRedis(Jedis jedis, String tableName, String pk) throws IOException {
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + pk;
        jedis.del(redisKey);
    }

    public static void setDimInfoFromRedis(JSONObject jsonData, Jedis jedis, String tableName, String pk) throws IOException {
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + pk;
        jedis.setex(redisKey, Common.REDIS_EXPIRE_SECONDS, jsonData.toJSONString());
    }
}
