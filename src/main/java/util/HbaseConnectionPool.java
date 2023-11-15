package util;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/15 20:24
 * @Function:
 */
public class HbaseConnectionPool {
    static GenericObjectPool<Connection> hbasePool;
    static {
        HbaseConnectionFactory hbaseFactory = new HbaseConnectionFactory();
        hbasePool = new GenericObjectPool<>(hbaseFactory);
        hbasePool.setMaxIdle(Common.HBASE_POOL_MAX_IDLE); // 设置最大空闲对象数为 5
        hbasePool.setMaxTotal(Common.HBASE_POOL_MAX_ACTIVE); // 设置最大活跃对象数为 10
    }

    public static Connection getConnection() throws Exception {
        return hbasePool.borrowObject();
    }

    public static void returnConnection(Connection connection) {
        hbasePool.returnObject(connection);
    }
}
