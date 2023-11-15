package util;

import bean.TableProcess;
import org.apache.commons.beanutils.BeanUtils;

import java.io.Serializable;
import java.lang.reflect.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.CaseFormat;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/4 11:33
 */
public class JdbcUtil {

    public static boolean isJavaBean(Class<?> clazz) {
        // 检查是否有公共的无参构造器
        try {
            Constructor<?> constructor = clazz.getConstructor();
            if (!Modifier.isPublic(constructor.getModifiers())) {
                return false;
            }
        } catch (NoSuchMethodException e) {
            return false;
        }
        // 检查是否有私有的属性和公共的getter和setter方法
        for (Field field : clazz.getDeclaredFields()) {
            // 忽略静态和瞬态的属性
            if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
                continue;
            }
//            // 检查属性是否是私有的
//            if (!Modifier.isPrivate(field.getModifiers())) {
//                return false;
//            }
            // 检查是否有公共的getter方法
            String getterName = "get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
            try {
                Method getter = clazz.getMethod(getterName);
                if (!Modifier.isPublic(getter.getModifiers())) {
                    return false;
                }
            } catch (NoSuchMethodException e) {
                return false;
            }
            // 检查是否有公共的setter方法
            String setterName = "set" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
            try {
                Method setter = clazz.getMethod(setterName, field.getType());
                if (!Modifier.isPublic(setter.getModifiers())) {
                    return false;
                }
            } catch (NoSuchMethodException e) {
                return false;
            }
        }
        // 如果以上条件都满足，那么T就是一个javabean
        return true;
    }


    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clazz, boolean isUnderScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchFieldException {
        ArrayList<T> res = new ArrayList<>();

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        boolean isJavaBean = isJavaBean(clazz);
        while (resultSet.next()) {
            T t;
            t = clazz.newInstance();

            for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                String columnName = resultSet.getMetaData().getColumnName(i + 1).toLowerCase();
                Object colValue = resultSet.getObject(columnName);
                if (isUnderScoreToCamel)
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                if(isJavaBean)
                    BeanUtils.setProperty(t, columnName, colValue);
                else
                    t = clazz.cast(colValue);
            }
            res.add(t);
        }
        return res;
    }

    public static void main(String[] args) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        Connection conn = DriverManager.getConnection(Common.MYSQL_URL, "root", "000000");
        queryList(conn, "select * from gmall_config.table_process", TableProcess.class, true).forEach(System.out::println);
    }
}
