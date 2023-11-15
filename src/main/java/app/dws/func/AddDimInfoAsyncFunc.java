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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    List<String> finalPks;


    public AddDimInfoAsyncFunc(String _tableName) {
        tableName = _tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        asyncConnection = HBaseUtil.getAsyncConnection();
        redisConnection = JedisUtil.getAsyncRedisConnection();

        java.sql.Connection mysqlConn = java.sql.DriverManager.getConnection(
                util.Common.MYSQL_URL,
                util.Common.MYSQL_USERNAME,
                util.Common.MYSQL_PASSWORD);
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
        mysqlConn.close();
    }

    @Override
    public void close() throws Exception {
        asyncConnection.close();
        redisConnection.close();
    }

    public abstract String getPk(IN input);

    public abstract OUT join(IN input, JSONObject dimInfo);

    @Override
    public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        String rowKey = getPk(input);
        if (finalPks.size() > 0) {
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
}
