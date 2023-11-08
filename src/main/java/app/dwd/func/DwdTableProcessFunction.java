package app.dwd.func;

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
 * @Date: 2023/11/8 10:35
 * @Function:
 */
public class DwdTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcess, JSONObject> {
    MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    HashMap<String, TableProcess> cacheMap;


    public DwdTableProcessFunction(MapStateDescriptor<String, TableProcess> _mapStateDescriptor) {
        mapStateDescriptor = _mapStateDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception, IOException {
        Connection mysqlConn = DriverManager.getConnection(
                Common.MYSQL_URL,
                Common.MYSQL_USERNAME,
                Common.MYSQL_PASSWORD);
        org.apache.hadoop.hbase.client.Connection hbaseConn = HBaseUtil.getConnection();
        List<TableProcess> tableProcesses = queryList(
                mysqlConn,
                "select * from gmall_config.table_process where sink_type='dwd'",
                TableProcess.class,
                true);

        cacheMap = new HashMap<>();
        for (TableProcess tableProcess : tableProcesses) {
            cacheMap.put(tableProcess.getSourceType() + tableProcess.getSourceTable(), tableProcess);
        }
        mysqlConn.close();
    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        /*
         * {"database":"gmall-220623-flink","table":"comment_info","type":"insert","ts":1669162958,"xid":1111,"xoffset":13941,"data":{"id":1595211185799847960,"user_id":119,"nick_name":null,"head_img":null,"sku_id":31,"spu_id":10,"order_id":987,"appraise":"1204","comment_txt":"评论内容：48384811984748167197482849234338563286217912223261","create_time":"2022-08-02 08:22:38","operate_time":null}}
         * */
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("type") + value.getString("table");
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess == null) {
            tableProcess = cacheMap.get(key);
        }
        if (tableProcess != null) {
            JSONObject data = value.getJSONObject("data");
            filterColumns(data, tableProcess.getSinkColumns());
            data.put("sink_topic", tableProcess.getSinkTable());
            out.collect(data);
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        List<String> columns = Arrays.stream(sinkColumns.split(",")).collect(Collectors.toList());
        data.entrySet().removeIf(entry -> !columns.contains(entry.getKey()));
    }

    @Override
    public void processBroadcastElement(TableProcess value, BroadcastProcessFunction<JSONObject, TableProcess, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        /*
         * {"before":null,"after":{"source_table":"base_category3","sink_table":"dim_base_category3","sink_columns":"id,name,category2_id","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1669162876406,"snapshot":"false","db":"gmall-220623-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1669162876406,"transaction":null}
         * */
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        if ("dwd".equals(value.getSinkType())) {
            String key = value.getSourceType() + value.getSourceTable();
            if ("d".equals(value.getOp())) {
                broadcastState.remove(key);
                cacheMap.remove(key);
            } else {
                broadcastState.put(key, value);
            }
        }

    }
}
