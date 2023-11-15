package app.dws.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;
import util.DimRedisUtil;
import util.HBaseUtil;
import util.JedisUtil;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static util.JdbcUtil.queryList;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/14 15:14
 * @Function:
 */
public abstract class AddDimFromRedisMapFunc<IN, OUT> extends RichMapFunction<IN, OUT> {

    Jedis jedis;
    Connection hbaseConn;
    String tableName;
    List<String> finalPks;

    public AddDimFromRedisMapFunc(String _tableName) {
        tableName = _tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = JedisUtil.getJedis();
        hbaseConn = HBaseUtil.getConnection();

        java.sql.Connection mysqlConn = java.sql.DriverManager.getConnection(
                util.Common.MYSQL_URL,
                util.Common.MYSQL_USERNAME,
                util.Common.MYSQL_PASSWORD);
        List<JSONObject> repKeyList = queryList(
                mysqlConn,
                "select sink_extend from gmall_config.table_process where sink_table='" + tableName + "';",
                JSONObject.class,
                true);
        if (repKeyList.size() > 0) {
            String sinkExtend = repKeyList.get(0).getString("sink_extend");
            String[] pks = sinkExtend.split(",");
            finalPks = Arrays.stream(pks)
                    .map(t -> t.replace("|", "_")) //也可以获得|的asc值，然后-1
                    .collect(Collectors.toList());
            finalPks.add(pks[pks.length - 1]);
        }
        mysqlConn.close();
    }

    @Override
    public void close() throws Exception {
        jedis.close();
        hbaseConn.close();
    }

    @Override
    public OUT map(IN value) throws Exception {
        String rowKey = getDimPk(value);
        if (finalPks.size() > 0) {
            rowKey = finalPks.get(rowKey.hashCode() % finalPks.size()) + rowKey;
        }
        JSONObject dimInfo = DimRedisUtil.getDimInfoFromRedisOrHbase(
                hbaseConn,
                jedis,
                tableName,
                rowKey);
        return join(value, dimInfo);
    }

    public abstract String getDimPk(IN value);

    public abstract OUT join(IN value, JSONObject dimInfo) throws Exception;
}
