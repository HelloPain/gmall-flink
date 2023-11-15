package app.dws.func;

import bean.TableProcess;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import util.Common;
import util.DimRedisUtil;
import util.HBaseUtil;
import util.JedisUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static util.JdbcUtil.queryList;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/14 18:38
 * @Function:
 */
public abstract class AddDimFromRedisProcessFunc<IN, OUT> extends BroadcastProcessFunction<IN, TableProcess, OUT> {
    MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    Jedis jedis;
    org.apache.hadoop.hbase.client.Connection hbaseConn;
    String tableName;
    String hbaseRepKey;

    public AddDimFromRedisProcessFunc(MapStateDescriptor<String, TableProcess> _mapStateDescriptor, String _tableName) {
        mapStateDescriptor = _mapStateDescriptor;
        tableName = _tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //冷启动: 第一次启动的时候，有可能state中没有数据，可以从mysql先加载数据到state中
        jedis = JedisUtil.getJedis();
        hbaseConn = HBaseUtil.getConnection();

        Connection mysqlConn = DriverManager.getConnection(Common.MYSQL_URL, Common.MYSQL_USERNAME, Common.MYSQL_PASSWORD);
        List<String> repKeyList = queryList(
                mysqlConn,
                "select sink_extend from gmall_config.table_process where sink_table='" + tableName + "';",
                String.class,
                true);
        hbaseRepKey = repKeyList.get(0);
        mysqlConn.close();
    }

    @Override
    public void close() throws Exception {
        jedis.close();
        hbaseConn.close();
    }

    public abstract String getDimPk(IN value);

    public abstract OUT join(IN value, JSONObject dimInfo) throws Exception;

    @Override
    public void processElement(IN value,
                               BroadcastProcessFunction<IN, TableProcess, OUT>.ReadOnlyContext ctx,
                               Collector<OUT> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tp = broadcastState.get(tableName);
        String repKey;
        if (tp != null) {
            repKey = tp.getSinkExtend();
        } else {
            repKey = hbaseRepKey;
        }

        JSONObject dimInfo = DimRedisUtil.getDimInfoFromRedisOrHbase(
                hbaseConn,
                jedis,
                tableName,
                HBaseUtil.getRowKey(getDimPk(value), repKey));
        out.collect(join(value, dimInfo));
    }

    @Override
    public void processBroadcastElement(TableProcess value, BroadcastProcessFunction<IN, TableProcess, OUT>.Context ctx, Collector<OUT> out) throws Exception {
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        if ("dim".equals(value.getSinkType())) {
            if ("d".equals(value.getOp())) {
                broadcastState.remove(value.getSourceTable());
            } else {
                broadcastState.put(value.getSourceTable(), value);
            }
        }
    }
}
