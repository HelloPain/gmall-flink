package app.dwd;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.Common;
import util.FlinkSqlUtil;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/7 10:16
 * @Function: Read topic_db data from kafka,
 *            extract `cart add` data from topic_db,
 *            sink to kafka
 * @DataLink: mock -> maxwell -> kafka(topic_db) -> flink table -> kafka(dwd_cart_add)
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(FlinkSqlUtil.createTopicDBFlinkTable());
        //tableEnv.sqlQuery("select * from ods_topic_db").execute().print();

        Table cart_add = tableEnv.sqlQuery("select\n" +
                        "`data`['id'] id,\n" +
                        "`data`['user_id'] user_id,\n" +
                        "`data`['sku_id'] sku_id,\n" +
                        "`data`['cart_price'] cart_price,\n" +
                        "if(`type`='insert',cast(`data`['sku_num'] as int),cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)) sku_num,\n" +
                        "`data`['sku_name'] sku_name,\n" +
                        "`data`['is_checked'] is_checked,\n" +
                        "`data`['create_time'] create_time,\n" +
                        "`data`['operate_time'] operate_time,\n" +
                        "`data`['is_ordered'] is_ordered,\n" +
                        "`data`['order_time'] order_time,\n" +
                        "`data`['source_type'] source_type,\n" +
                        "`data`['source_id'] source_id\n" +
                        "from ods_topic_db\n" +
                        "where  `database` = 'gmall-flink'\n" +
                        "      and `table` = 'cart_info'\n" +
                        "      and `type` = 'insert' " +
                        "or (`type`='update' " +
                        "and `old`['sku_num'] is not null and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int));");
        tableEnv.createTemporaryView("cart_add", cart_add);
        //tableEnv.sqlQuery("select * from cart_add").execute().print();

        tableEnv.executeSql("" +
                "create table dwd_cart_info(\n" +
                "`id` string,\n" +
                "`user_id` string,\n" +
                "`sku_id` string,\n" +
                "`cart_price` string,\n" +
                "`sku_num` int,\n" +
                "`sku_name` string,\n" +
                "`is_checked` string,\n" +
                "`create_time` string,\n" +
                "`operate_time` string,\n" +
                "`is_ordered` string,\n" +
                "`order_time` string,\n" +
                "`source_type` string,\n" +
                "`source_id` string\n" +
                ")\n" + FlinkSqlUtil.getKafkaProducerDDL(Common.TOPIC_DWD_TRADE_CART_ADD));

        tableEnv.executeSql("insert into dwd_cart_info select * from cart_add");

        //env.execute();
    }
}
