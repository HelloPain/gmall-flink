package app.lab;

import bean.TableProcess;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.Common;
import util.HBaseUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static util.JdbcUtil.queryList;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/4 10:19
 */
public class DimAndDwdTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcess, JSONObject> {
    MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    HashMap<String, TableProcess> cacheMap;
    OutputTag<JSONObject> dwdOutputTag;

    public DimAndDwdTableProcessFunction(MapStateDescriptor<String, TableProcess> _mapStateDescriptor, OutputTag<JSONObject> _dwdOutputTag) {
        mapStateDescriptor = _mapStateDescriptor;
        dwdOutputTag = _dwdOutputTag;
    }


    @Override
    public void open(Configuration parameters) throws Exception, IOException {
        //第一次启动的时候，有可能state中没有数据，可以从mysql先加载数据到state中
        //为了冷启动？
        Connection mysqlConn = DriverManager.getConnection(Common.MYSQL_URL, Common.MYSQL_USERNAME, Common.MYSQL_PASSWORD);
        org.apache.hadoop.hbase.client.Connection hbaseConn = HBaseUtil.getConnection();
        List<TableProcess> tableProcesses = queryList(
                mysqlConn,
                "select * from gmall_config.table_process",
                TableProcess.class,
                true);

        cacheMap = new HashMap<>();
        for (TableProcess tableProcess : tableProcesses) {
            if ("dim".equals(tableProcess.getSinkType())) {
                HBaseUtil.createTable(
                        hbaseConn,
                        Common.HBASE_NAMESPACE,
                        tableProcess.getSinkTable(),
                        HBaseUtil.getSplitKeys(tableProcess.getSinkExtend()),
                        tableProcess.getSinkFamily().split(","));
                cacheMap.put(tableProcess.getSourceTable(), tableProcess);
            } else {
                cacheMap.put(tableProcess.getSourceType() + tableProcess.getSourceTable(), tableProcess);
            }
        }
        mysqlConn.close();
    }

    @Override
    public void processElement(JSONObject value,
                               BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.ReadOnlyContext ctx,
                               Collector<JSONObject> out) throws Exception {
        /*
         * {"database":"gmall-220623-flink","table":"comment_info","type":"insert","ts":1669162958,"xid":1111,"xoffset":13941,"data":{"id":1595211185799847960,"user_id":119,"nick_name":null,"head_img":null,"sku_id":31,"spu_id":10,"order_id":987,"appraise":"1204","comment_txt":"评论内容：48384811984748167197482849234338563286217912223261","create_time":"2022-08-02 08:22:38","operate_time":null}}
         * */

        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //user info table can be dwd and dim simultaneously, it has two table config item
        List<TableProcess> tableProcessList = new ArrayList<>();

        //get table config for dim table
        String dimKey = value.getString("table");
        TableProcess dimTableProcess = broadcastState.get(dimKey);
        if (dimTableProcess == null) {
            dimTableProcess = cacheMap.get(dimKey);
        }
        if (dimTableProcess != null) {
            tableProcessList.add(dimTableProcess);
        }

        //get table config for dwd table
        String dwdKey = value.getString("type") + value.getString("table");
        TableProcess dwdTableProcess = broadcastState.get(dwdKey);
        if (dwdTableProcess == null) {
            dwdTableProcess = cacheMap.get(dwdKey);
        }
        if (dwdTableProcess != null) {
            tableProcessList.add(dwdTableProcess);
        }

        //if table config is not null, then filter columns and output
        for (TableProcess tableProcess : tableProcessList) {
            if ("dim".equals(tableProcess.getSinkType())) {
                filterColumns(value.getJSONObject("data"), tableProcess.getSinkColumns());
                value.put("sink_table", tableProcess.getSinkTable());
                value.put("row_key_field", tableProcess.getSinkRowKey());
                value.put("sink_columns", tableProcess.getSinkColumns());
                value.put("sink_column_family", tableProcess.getSinkFamily());
                value.put("sink_extend", tableProcess.getSinkExtend());
                out.collect(value);
            } else {
                JSONObject data = value.getJSONObject("data");
                filterColumns(data, tableProcess.getSinkColumns());
                data.put("sink_topic", tableProcess.getSinkTable());
                ctx.output(dwdOutputTag, data);
            }
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
