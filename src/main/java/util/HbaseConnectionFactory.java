package util;

import org.apache.hadoop.hbase.client.*;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/15 13:56
 * @Function:
 */
public class HbaseConnectionFactory extends BasePooledObjectFactory<Connection>{
    @Override
    public Connection create() throws Exception {
        return HBaseUtil.getConnection();
    }

    @Override
    public PooledObject<Connection> wrap(Connection conn) {
        return new DefaultPooledObject<>(conn);
    }
}
