package util;

import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/16 15:13
 * @Function:
 */
public class HBaseDataSourceUtil {

    private LinkedList<Connection> connectionList;

    public HBaseDataSourceUtil() throws IOException {
        connectionList = new LinkedList<>();
        for (int i = 0; i < 5; i++) {
            connectionList.add(HBaseUtil.getConnection());
        }
    }

    public Connection getConnection() throws IOException {
        synchronized (HBaseDataSourceUtil.class) {
            if (connectionList.size() > 0) {
                return connectionList.removeFirst();
            } else {
                return HBaseUtil.getConnection();
            }
        }
    }

    public void addBack(Connection connection) {
        synchronized (HBaseDataSourceUtil.class) {
            connectionList.add(connection);
        }
    }

    public static void main(String[] args) throws IOException {
        HBaseDataSourceUtil hBaseDataSourceUtil = new HBaseDataSourceUtil();

        ThreadPoolExecutor threadPool = ThreadPoolUtil.getThreadPoolExecutor();
        for (int i = 0; i < 10000; i++) {
            int finalI = i;
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection connection = hBaseDataSourceUtil.getConnection();
                        //output thread name:
                        //System.out.println(finalI +":");
                        //System.out.println(connection);
                        hBaseDataSourceUtil.addBack(connection);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
