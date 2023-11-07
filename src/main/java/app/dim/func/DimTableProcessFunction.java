package app.dim.func;

import bean.TableProcess;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import util.Common;
import util.HBaseUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static util.JdbcUtil.queryList;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/4 10:19
 */
public class DimTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcess, JSONObject> {
    MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    HashMap<String, TableProcess> cacheMap;
    Integer count = 0;

    public DimTableProcessFunction(MapStateDescriptor<String, TableProcess> _mapStateDescriptor) {
        mapStateDescriptor = _mapStateDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception, IOException {
        //第一次启动的时候，有可能state中没有数据，可以从mysql先加载数据到state中
        //为了冷启动？
        Connection mysqlConn = DriverManager.getConnection(Common.MYSQL_URL, Common.MYSQL_USERNAME, Common.MYSQL_PASSWORD);
        org.apache.hadoop.hbase.client.Connection hbaseConn = HBaseUtil.getConnection();
        List<TableProcess> tableProcesses = queryList(mysqlConn,
                "select * from gmall_config.table_process where sink_type='dim'"
                , TableProcess.class, true);

        cacheMap = new HashMap<>();
        for (TableProcess tableProcess : tableProcesses) {
            HBaseUtil.createTable(
                    hbaseConn, Common.HBASE_NAMESPACE, tableProcess.getSinkTable(),
                    HBaseUtil.getSplitKeys(tableProcess.getSinkExtend()),
                    tableProcess.getSinkFamily().split(","));
            cacheMap.put(tableProcess.getSourceTable(), tableProcess);
        }
        mysqlConn.close();
    }

    @Override
    public void processElement(JSONObject value,
                               BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.ReadOnlyContext ctx,
                               Collector<JSONObject> out) throws Exception {
        //第一次启动的时候，有可能state中没有数据，可以从mysql先加载数据到state中
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(value.getString("table"));
        if (tableProcess == null) {
            tableProcess = cacheMap.get(value.getString("table"));
        }
        if (tableProcess != null) {
            filterColumns(value.getJSONObject("data"), tableProcess.getSinkColumns());
            value.put("sink_table", tableProcess.getSinkTable());
            value.put("row_key_field", tableProcess.getSinkRowKey());
            value.put("sink_columns", tableProcess.getSinkColumns());
            value.put("sink_column_family", tableProcess.getSinkFamily());
            value.put("sink_extend", tableProcess.getSinkExtend());
            //System.out.println("value = " + value);
            System.out.println("DimTableProcessFunction.count = " + ++count);
            out.collect(value);
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        List<String> columns = Arrays.stream(sinkColumns.split(",")).collect(Collectors.toList());
        data.entrySet().removeIf(entry -> !columns.contains(entry.getKey()));
    }

    @Override
    public void processBroadcastElement(TableProcess value, BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        if ("dim".equals(value.getSinkType())) {
            if ("d".equals(value.getOp())) {
                broadcastState.remove(value.getSourceTable());
                cacheMap.remove(value.getSourceTable());
            } else {
                broadcastState.put(value.getSourceTable(), value);
            }
        }
    }
}
