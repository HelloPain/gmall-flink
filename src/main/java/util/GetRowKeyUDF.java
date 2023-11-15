package util;

import bean.TableProcess;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/7 9:45
 */
public class GetRowKeyUDF extends ScalarFunction {
    HashMap<String, String> sinkExtends;

    @Override
    public void open(FunctionContext context) throws Exception {
        Connection mysqlConn = DriverManager.getConnection(
                Common.MYSQL_URL,
                Common.MYSQL_USERNAME,
                Common.MYSQL_PASSWORD);
        sinkExtends = new HashMap<>();
        JdbcUtil.queryList(mysqlConn,
                        "select * from gmall_config.table_process"
                        , TableProcess.class, true)
                .forEach(t -> sinkExtends.put(t.getSinkTable(), t.getSinkExtend()));
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    JdbcUtil.queryList(mysqlConn,
                                    "select * from gmall_config.table_process"
                                    , TableProcess.class, true)
                            .forEach(t -> sinkExtends.put(t.getSinkTable(), t.getSinkExtend()));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }
            }
        },10000L,10000L);//check if mysql has changed every delay, update sinkExtends
    }

    public String eval(String rowKey, String tableName) {
        String sinkExtend = sinkExtends.get(tableName);
        if(sinkExtend == null) return rowKey;
        return HBaseUtil.getRowKey(rowKey, sinkExtend);
    }

}
