package app.dim.func;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import util.Common;
import util.HBaseUtil;
import org.apache.hadoop.hbase.client.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/4 14:14
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getConnection();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //System.out.println("\"invoke\" = " + "invoke");
        JSONObject data = value.getJSONObject("data");
        String type = value.getString("type");
        String hbaseTableName = value.getString("sink_table");
        String rowkeyField = value.getString("row_key_field");
        String rowkey = data.getString(rowkeyField);
        //if rowkey is repartition
        if (rowkeyField != null) {
            rowkey = getRowKey(rowkey, value.getString("sink_extend"));
        }
        if ("delete".equals(type)) {
            HBaseUtil.deleteData(conn, Common.HBASE_NAMESPACE, hbaseTableName, rowkey);
        } else {
            HBaseUtil.putJsonData(conn, Common.HBASE_NAMESPACE, hbaseTableName, rowkey,
                    value.getString("sink_column_family"), data);
        }
    }

    //sinkExtend: 00|,01|,02|
    //rowKey: 1001 1002...
    //rowKey: 00_1000 01_1001 02_1002 02|1003
    public static String getRowKey(String rowKey, String sinkExtend) {
        String[] pks = sinkExtend.split(",");
        List<String> final_pks = Arrays.stream(pks)
                .map(t -> t.replace("|", "_")) //也可以获得|的asc值，然后-1
                .collect(Collectors.toList());
        final_pks.add(pks[pks.length - 1]); //00_,01_,02_,02|
        return final_pks.get(rowKey.hashCode() % final_pks.size()) + rowKey;
    }

    @Override
    public void close() throws Exception {
        conn.close();
    }

    public static void main(String[] args) {
        System.out.println(getRowKey("1001", "00|,01|,02|"));
        System.out.println(getRowKey("1002", "00|,01|,02|"));
        System.out.println(getRowKey("1003", "00|,01|,02|"));
        System.out.println(getRowKey("1004", "00|,01|,02|"));
    }

}
