package app.dws.func;

import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;
import util.Common;
import util.DimRedisUtil;
import util.HBaseUtil;
import util.JedisUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static util.JdbcUtil.queryList;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/15 15:18
 * @Function:
 */
public abstract class AddDimInfoAsyncFunc<IN, OUT> extends RichAsyncFunction<IN, OUT> {

    private AsyncConnection asyncConnection;
    private StatefulRedisConnection<String, String> redisConnection;
    private final String tableName;
    private List<String> finalPks;
    private java.sql.Connection mysqlConn;


    public AddDimInfoAsyncFunc(String _tableName) {
        tableName = _tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        asyncConnection = HBaseUtil.getAsyncConnection();
        redisConnection = JedisUtil.getAsyncRedisConnection();

        mysqlConn = java.sql.DriverManager.getConnection(
                util.Common.MYSQL_URL,
                util.Common.MYSQL_USERNAME,
                util.Common.MYSQL_PASSWORD);
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
        }, 60 * 1000 * 60 * 24L, 60 * 1000 * 60 * 24L);
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
        asyncConnection.close();
        redisConnection.close();
        mysqlConn.close();
    }

    public abstract String getPk(IN input);

    public abstract OUT join(IN input, JSONObject dimInfo);

    @Override
    public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        String rowKey = getPk(input);
        if (finalPks != null && finalPks.size() > 0) {
            rowKey = finalPks.get(rowKey.hashCode() % finalPks.size()) + rowKey;
        }
        String finalRowKey = rowKey;
        CompletableFuture.supplyAsync(new Supplier<JSONObject>() {
            @SneakyThrows
            @Override
            public JSONObject get() {
                return DimRedisUtil.asyncTryGetDimInfoFromRedis(redisConnection, tableName, finalRowKey);
            }
        }).thenApplyAsync(new Function<JSONObject, JSONObject>() {
            @SneakyThrows
            @Override
            public JSONObject apply(JSONObject jsonObject) {
                if (jsonObject != null) { //在Redis已经查询到了数据
                    return jsonObject;
                } else { //在Redis没有查询到数据,则访问HBase,并将数据写到Redis
                    JSONObject dimInfoJson = HBaseUtil.getJsonDataAsync(
                            asyncConnection,
                            Common.HBASE_NAMESPACE,
                            tableName,
                            finalRowKey);
                    DimRedisUtil.asyncSetDimInfoFromRedis(
                            redisConnection,
                            tableName,
                            finalRowKey,
                            dimInfoJson);
                    return dimInfoJson;
                }
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject jsonObject) {
                //将维表信息补充到数据上
                resultFuture.complete(Collections.singletonList(join(input, jsonObject)));
            }
        });
    }

    @Override
    public void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        System.out.println("TimeOut>>>>>>" + input);
    }
}
