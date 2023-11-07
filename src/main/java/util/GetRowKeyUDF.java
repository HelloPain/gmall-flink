package util;

import bean.TableProcess;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/7 9:45
 */
public class GetRowKeyUDF extends ScalarFunction {
    HashMap<String, String> sinkExtends;

    @Override
    public void open(FunctionContext context) throws Exception {
        Connection mysqlConn = DriverManager.getConnection(Common.MYSQL_URL, Common.MYSQL_USERNAME, Common.MYSQL_PASSWORD);
        sinkExtends = new HashMap<>();
        JdbcUtil.queryList(mysqlConn,
                        "select * from gmall_config.table_process"
                        , TableProcess.class, true)
                .forEach(t -> sinkExtends.put(t.getSinkTable(), t.getSinkExtend()));
    }

    public String eval(String rowKey, String tableName) {
        String[] pks = sinkExtends.get(tableName).split(",");
        List<String> final_pks = Arrays.stream(pks)
                .map(t -> t.replace("|", "_")) //也可以获得|的asc值，然后-1
                .collect(Collectors.toList());
        final_pks.add(pks[pks.length - 1]); //00_,01_,02_,02|
        return final_pks.get(rowKey.hashCode() % final_pks.size()) + rowKey;
    }

}
