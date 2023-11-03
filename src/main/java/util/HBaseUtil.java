package util;

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

    public static void createTable(Connection conn, String namespace, String tableName, byte[][] splitKeys, String... colFamilies) throws IOException {
        if (colFamilies.length < 1) {
            throw new RuntimeException("Column families can not be empty.");
        }

        Admin admin = conn.getAdmin();
        TableName tN = TableName.valueOf(namespace + ":" + tableName);
        if (admin.tableExists(tN)) {
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
}
