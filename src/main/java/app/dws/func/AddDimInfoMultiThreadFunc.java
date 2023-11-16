package app.dws.func;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;
import util.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static util.JdbcUtil.queryList;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/15 15:18
 * @Function:
 */
public abstract class AddDimInfoMultiThreadFunc<IN, OUT> extends RichAsyncFunction<IN, OUT> {
    final String tableName;
    List<String> finalPks;
    ThreadPoolExecutor threadPool;
    java.sql.Connection mysqlConn;


    public AddDimInfoMultiThreadFunc(String _tableName) {
        tableName = _tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPoolExecutor();

        mysqlConn = java.sql.DriverManager.getConnection(
                Common.MYSQL_URL,
                Common.MYSQL_USERNAME,
                Common.MYSQL_PASSWORD);
        updateRepartitionKey();
        //check if mysql has changed every delay, update repartition key
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    updateRepartitionKey();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 10000L, 10000L);
    }

    private void updateRepartitionKey() throws SQLException, NoSuchFieldException, InvocationTargetException, InstantiationException, IllegalAccessException {
        List<String> repKeyList = queryList(
                mysqlConn,
                "select sink_extend from gmall_config.table_process where sink_table='" + tableName + "';",
                String.class,
                true);
        if (repKeyList.size() > 0) {
            String sinkExtend = repKeyList.get(0);
            String[] pks = sinkExtend.split(",");
            finalPks = Arrays.stream(pks)
                    .map(t -> t.replace("|", "_")) //也可以获得|的asc值，然后-1
                    .collect(Collectors.toList());
            finalPks.add(pks[pks.length - 1]);
        }
    }

    @Override
    public void close() throws Exception {
        mysqlConn.close();
    }

    public abstract String getPk(IN input);

    public abstract OUT join(IN input, JSONObject dimInfo);

    @Override
    public void asyncInvoke(IN value, ResultFuture<OUT> resultFuture) throws Exception {
        String rowKey = getPk(value);
        if (finalPks.size() > 0) {
            rowKey = finalPks.get(rowKey.hashCode() % finalPks.size()) + rowKey;
        }
        String finalRowKey = rowKey;
        threadPool.execute(new Runnable() {
            @Override
            @SneakyThrows
            public void run() {
                try {
                    Connection hbaseConn = HbaseConnectionPool.getConnection();
                    Jedis jedisConn = JedisUtil.getJedis();
                    JSONObject dimInfo = DimRedisUtil.getDimInfoFromRedisOrHbase(
                            hbaseConn,
                            jedisConn,
                            tableName,
                            finalRowKey);
                    HbaseConnectionPool.returnConnection(hbaseConn);
                    jedisConn.close(); //return back to jedis pool
                    resultFuture.complete(Collections.singletonList(join(value, dimInfo)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        System.out.println("TimeOut>>>>>>" + input);
    }
}
