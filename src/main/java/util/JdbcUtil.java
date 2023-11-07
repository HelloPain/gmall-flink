package util;

import bean.TableProcess;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import com.google.common.base.CaseFormat;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/4 11:33
 */
public class JdbcUtil {

    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clazz, boolean isUnderScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ArrayList<T> res = new ArrayList<>();

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while(resultSet.next()){
            T t;
            t = clazz.newInstance();

            for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                String columnName = resultSet.getMetaData().getColumnName(i + 1).toLowerCase();
                Object colValue = resultSet.getObject(columnName);
                if(isUnderScoreToCamel)
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                BeanUtils.setProperty(t, columnName, colValue);
            }
            res.add(t);
        }
        return res;
    }

    public static void main(String[] args) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Connection conn = DriverManager.getConnection(Common.MYSQL_URL,"root", "000000");
        queryList(conn, "select * from gmall_config.table_process", TableProcess.class,true).forEach(System.out::println);
    }
}
