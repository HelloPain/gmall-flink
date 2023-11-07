package util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/3 15:17
 */
public class HBaseUtil {
    public static Connection getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();//load hbase-site.xml
        return ConnectionFactory.createConnection(conf);
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
        System.out.println("\"put json data\" = " + data);
        Table table = conn.getTable(TableName.valueOf(namespace + ":" + tableName));
        Put put = new Put(rowKey.getBytes());
        for (String key : data.keySet()) {
            String val = data.getString(key);
            if (val != null)
                put.addColumn(colFamily.getBytes(), key.getBytes(), val.getBytes());
        }
        System.out.println("\"put:\"+rowKey = " + rowKey);
        table.put(put);
        table.close();
    }

    public static void deleteData(Connection conn, String namespace, String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(namespace + ":" + tableName));
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
        table.close();
    }

    public static byte[][] getSplitKeys(String sinkExtend){
        if(sinkExtend == null || sinkExtend.length() == 0){
            return null;
        }
        String[] splits = sinkExtend.split(",");//00|,01|,02|
        byte[][] bytes = new byte[splits.length][];
        for (int i = 0; i < splits.length; i++) {
            bytes[i] = splits[i].getBytes();
        }
        return bytes;
    }
}
