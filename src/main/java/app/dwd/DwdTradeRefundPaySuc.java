package app.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.Common;
import util.FlinkSqlUtil;
import util.GetRowKeyUDF;

import java.time.Duration;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/7 10:46
 * @Function: 从 Kafka 读取业务数据，筛选退单表数据，筛选满足条件的订单表数据，建立 HBase-Lookup 字典表，关联三张表获得退单明细宽表。
 *            Read topic_db from kafka,
 *            Extract `order_info` (filter cancel order),`order_detail`,`order_detail_activity`,`order_detail_coupon` data from topic_db, join them,
 *            Sink to kafka
 * @DataLink: mock -> maxwell -> kafka(topic_db) -> flink table -> kafka(dwd_trade_cancel_detail)
 */
public class DwdTradeRefundPaySuc {
    public static void main(String[] args) throws Exception {
        //1.read from kafka topic_db, turn it into flink table
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(FlinkSqlUtil.createTopicDBFlinkTable());
        //tableEnv.sqlQuery("select * from ods_topic_db").execute().print();

        //set TTL for join
        //TableConfig config = tableEnv.getConfig();
        //config.setIdleStateRetention(Duration.ofSeconds(10));

        //2.get refund order info from topic_db
        Table orderInfo = tableEnv.sqlQuery(
                   "select\n" +
                        "data['id'] id,\n" +
                        "data['province_id'] province_id\n" +
                   "from ods_topic_db\n" +
                   "where `database` = 'gmall_flink'\n" +
                   "      and `table` = 'order_info'\n" +
                   "      and `type` = 'update'\n" +
                   "      and `old`['order_status'] is not null\n" +
                   "      and `data`['order_status'] = '1006';");
        tableEnv.createTemporaryView("order_refund_info", orderInfo);
        //tableEnv.sqlQuery("select * from order_refund_info").execute().print();

        Table refundPayment = tableEnv.sqlQuery(
                "select\n" +
                        "data['id'] id,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['sku_id'] sku_id,\n" +
                        "data['payment_type'] payment_type,\n" +
                        "data['callback_time'] callback_time,\n" +
                        "data['total_amount'] total_amount,\n" +
                        "pt,\n" +
                        "ts\n" +
                "from ods_topic_db\n" +
                        "where `database` = 'gmall_flink'\n" +
                        "and `table` = 'refund_payment'\n" +
                        "and `type` = 'update'\n"
 //                       "and data['refund_status'] = '0701'\n" + //数据里没有0701
 //                       "and `old`['refund_status'] is not null"
        );
        tableEnv.createTemporaryView("refund_payment", refundPayment);
        //tableEnv.sqlQuery("select * from refund_payment").execute().print();


        tableEnv.createTemporarySystemFunction("getRowKey", GetRowKeyUDF.class);
        Table orderDetail = tableEnv.sqlQuery(
                "select\n" +
                    "data['id'] id,\n" +
                    "data['user_id'] user_id,\n" +
                    "data['order_id'] order_id,\n" +
                    "data['sku_id'] sku_id,\n" +
                    "getRowKey(data['refund_type'],'dim_base_dic') refund_type,\n" +
                    "data['refund_num'] refund_num,\n" +
                    "data['refund_amount'] refund_amount,\n" +
                    "getRowKey(data['refund_reason_type'],'dim_base_dic') refund_reason_type,\n" +
                    "data['refund_reason_txt'] refund_reason_txt,\n" +
                    "data['create_time'] create_time,\n" +
                    "ts,\n"+
                    "pt\n" +
                "from ods_topic_db\n" +
                "where `database` = 'gmall_flink'\n" +
                "      and `table` = 'order_refund_info'\n" +
                "      and `type` = 'insert';");
        tableEnv.createTemporaryView("order_refund_detail", orderDetail);
        //tableEnv.sqlQuery("select * from order_refund_detail").execute().print();

        tableEnv.executeSql("CREATE TABLE dim_base_dic (\n" +
                "    rowkey string,\n" +
                "    info ROW<dic_name string>\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-2.2',\n" +
                "    'table-name' = 'gmall_flink:dim_base_dic',\n" +
                "    'zookeeper.quorum' = 'hadoop102:2181'\n" +
                ")");
        //tableEnv.sqlQuery("select * from dim_base_dic").execute().print();

        //3.look up join
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                    "ord.id,\n" +
                    "ord.user_id,\n" +
                    "ord.order_id,\n" +
                    "ord.sku_id,\n" +
                    "oi.province_id,\n" +
                    "date_format(ord.create_time,'yyyy-MM-dd') date_id,\n" +
                    "ord.create_time,\n" +
                    "ord.refund_type refund_type_code,\n" +
                    "type_dic.info.dic_name refund_type_name,\n" +
                    "ord.refund_reason_type refund_reason_type_code,\n" +
                    "reason_dic.info.dic_name refund_reason_type_name,\n" +
                    "ord.refund_reason_txt,\n" +
                    "ord.refund_num,\n" +
                    "ord.refund_amount,\n" +
                    "ord.ts\n" +
                "from order_refund_detail ord\n" +
                "join order_refund_info oi\n" +
                "on ord.order_id = oi.id\n" +
                "join dim_base_dic FOR SYSTEM_TIME AS OF ord.pt as type_dic\n" +
                "on ord.refund_type = type_dic.rowkey\n"+
                "join dim_base_dic FOR SYSTEM_TIME AS OF ord.pt as reason_dic\n" +
                "on ord.refund_reason_type = reason_dic.rowkey\n");
        tableEnv.createTemporaryView("joined", resultTable);
        tableEnv.sqlQuery("select * from joined").execute().print();

        //4.sink to kafka
        tableEnv.executeSql(
                "create table dwd_trade_order_refund(\n" +
                            "id string,\n" +
                            "user_id string,\n" +
                            "order_id string,\n" +
                            "sku_id string,\n" +
                            "province_id string,\n" +
                            "date_id string,\n" +
                            "create_time string,\n" +
                            "refund_type_code string,\n" +
                            "refund_type_name string,\n" +
                            "refund_reason_type_code string,\n" +
                            "refund_reason_type_name string,\n" +
                            "refund_reason_txt string,\n" +
                            "refund_num string,\n" +
                            "refund_amount string,\n" +
                            "ts bigint,\n" +
                            "primary key(id) not enforced\n" +
                            ")" + FlinkSqlUtil.getUpsertKafkaProducerDDL(Common.TOPIC_DWD_TRADE_ORDER_REFUND));

        // TODO 9. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_order_refund select * from joined");


        //env.execute();
    }
}
