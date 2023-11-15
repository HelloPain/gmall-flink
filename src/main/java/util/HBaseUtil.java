package util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/3 15:17
 */
public class HBaseUtil {
    public static Connection getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();//load hbase-site.xml
        return ConnectionFactory.createConnection(conf);
    }

    public static AsyncConnection getAsyncConnection() throws ExecutionException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        return ConnectionFactory.createAsyncConnection(configuration).get();
    }

    public static void createTable(Connection conn, String namespace, String tableName,
                                   byte[][] splitKeys, String... colFamilies) throws IOException {
        if (colFamilies.length < 1) {
            throw new RuntimeException("Column families can not be empty.");
        }

        Admin admin = conn.getAdmin();
        TableName tN = TableName.valueOf(namespace + ":" + tableName);
        if (admin.tableExists(tN)) {
            //dropTable(conn, namespace, tableName);
            return;
        }

        ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        Arrays.stream(colFamilies).distinct().forEach(cf -> columnFamilyDescriptors.add(
                ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build()));
        TableDescriptorBuilder tableDescriptorBuilder =
                TableDescriptorBuilder.newBuilder(tN)
                        .setColumnFamilies(columnFamilyDescriptors);

        if (splitKeys != null) {
            admin.createTable(tableDescriptorBuilder.build(), splitKeys);
        } else {
            admin.createTable(tableDescriptorBuilder.build());
        }
        admin.close();
    }

    public static void dropTable(Connection conn, String namespace, String tableName) throws IOException {
        Admin admin = conn.getAdmin();
        TableName tN = TableName.valueOf(namespace + ":" + tableName);
        if (!admin.tableExists(tN)) {
            throw new RuntimeException("Hbase table does not exist.");
        }
        admin.disableTable(tN);
        admin.deleteTable(tN);
        admin.close();
    }

    public static void putData(Connection conn, String namespace, String tableName, String rowKey, String colFamily, String col, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(namespace + ":" + tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), value.getBytes());
        table.put(put);
        table.close();
    }

    public static void putJsonData(Connection conn, String namespace, String tableName, String rowKey, String colFamily, JSONObject data) throws IOException {
        //System.out.println("\"put json data\" = " + data);
        Table table = conn.getTable(TableName.valueOf(namespace + ":" + tableName));
        Put put = new Put(rowKey.getBytes());
        for (String key : data.keySet()) {
            String val = data.getString(key);
            if (val != null)
                put.addColumn(colFamily.getBytes(), key.getBytes(), val.getBytes());
        }
        //System.out.println("\"put:\"+rowKey = " + rowKey);
        table.put(put);
        table.close();
    }

    public static void deleteData(Connection conn, String namespace, String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(namespace + ":" + tableName));
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
        table.close();
    }

    public static byte[][] getSplitKeys(String sinkExtend) {
        if (sinkExtend == null || sinkExtend.length() == 0) {
            return null;
        }
        String[] splits = sinkExtend.split(",");//00|,01|,02|
        byte[][] bytes = new byte[splits.length][];
        for (int i = 0; i < splits.length; i++) {
            bytes[i] = splits[i].getBytes();
        }
        return bytes;
    }

    public static JSONObject getJsonData(Connection conn, String namespace, String tableName, String rowkey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(namespace + ":" + tableName));
        Get get = new Get(rowkey.getBytes());
        Result result = table.get(get);
        JSONObject jsonObject = new JSONObject();
        for (Cell cell : result.rawCells()) {
            jsonObject.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        table.close();
        return jsonObject;
    }

    public static JSONObject getJsonDataAsync(AsyncConnection conn, String namespace, String tableName, String rowkey) throws IOException, ExecutionException, InterruptedException {
        AsyncTable<AdvancedScanResultConsumer> asyncTable = conn.getTable(TableName.valueOf(namespace + ":" + tableName));
        Get get = new Get(rowkey.getBytes());
        Result result = asyncTable.get(get).get();
        JSONObject jsonObject = new JSONObject();
        for (Cell cell : result.rawCells()) {
            jsonObject.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        return jsonObject;
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

    public static void main(String[] args) throws IOException {
        Connection conn = getConnection();
        long t1 = System.currentTimeMillis();
        System.out.println(getJsonData(conn, "gmall_flink", "dim_base_trademark", "01_11"));
        long t2 = System.currentTimeMillis();
        System.out.println("t2-t1 = " + (t2 - t1));//1227

        System.out.println(getJsonData(conn, "gmall_flink", "dim_base_trademark", "01_11"));
        long t3 = System.currentTimeMillis();
        System.out.println("t3-t2 = " + (t3 - t2));//46

        Jedis jedis = JedisUtil.getJedis();
        System.out.println(DimRedisUtil.getDimInfoFromRedisOrHbase(conn, jedis, "dim_base_trademark", "01_11"));
        long t4 = System.currentTimeMillis();
        System.out.println("t4-t3 = " + (t4 - t3));

        System.out.println(DimRedisUtil.getDimInfoFromRedisOrHbase(conn, jedis, "dim_base_trademark", "01_11"));
        long t5 = System.currentTimeMillis();
        System.out.println("t5-t4 = " + (t5 - t4));

        System.out.println(DimRedisUtil.getDimInfoFromRedisOrHbase(conn, jedis, "dim_base_trademark", "01_11"));
        long t6 = System.currentTimeMillis();
        System.out.println("t6-t5 = " + (t6 - t5));
    }
}
