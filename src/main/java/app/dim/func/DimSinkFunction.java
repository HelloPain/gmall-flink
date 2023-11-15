package app.dim.func;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import util.Common;
import util.DimRedisUtil;
import util.HBaseUtil;
import org.apache.hadoop.hbase.client.*;
import util.JedisUtil;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/4 14:14
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection conn;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getConnection();
        jedis = JedisUtil.getJedis();
    }
    @Override
    public void close() throws Exception {
        conn.close();
        jedis.close();
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
            rowkey = HBaseUtil.getRowKey(rowkey, value.getString("sink_extend"));
        }
        if ("delete".equals(type)) {
            HBaseUtil.deleteData(conn, Common.HBASE_NAMESPACE, hbaseTableName, rowkey);
            DimRedisUtil.deleteDimInfoFromRedis(jedis, hbaseTableName, rowkey);
        } else {
            if("update".equals(type)){
                DimRedisUtil.setDimInfoFromRedis(data, jedis, hbaseTableName, rowkey);
            }
            HBaseUtil.putJsonData(conn, Common.HBASE_NAMESPACE, hbaseTableName, rowkey,
                    value.getString("sink_column_family"), data);
        }
    }




    public static void main(String[] args) {
        System.out.println(HBaseUtil.getRowKey("1001", "00|,01|,02|"));
        System.out.println(HBaseUtil.getRowKey("1002", "00|,01|,02|"));
        System.out.println(HBaseUtil.getRowKey("1003", "00|,01|,02|"));
        System.out.println(HBaseUtil.getRowKey("1004", "00|,01|,02|"));
    }

}
