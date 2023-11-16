package app.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.FlinkSqlUtil;
import util.Common;
import util.GetRowKeyUDF;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/11 16:45
 * @Function: 从 Kafka topic_db主题筛选支付成功数据、从dwd_trade_order_detail主题中读取订单事实数据、HBase-LookUp字典表，
 * 关联三张表形成支付成功宽表，写入 Kafka 支付成功主题。
 * @DataLink: DwdTradeOrderDetail: mock -> maxwell -> kafka(topic_db) -> flink table -> kafka(dwd_trade_pay_detail_suc)
 * kafka(dwd_trade_pay_detail_suc) + hbase -> flink table
 */
public class DwdTradePayDetailSuc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.read from kafka dwd_trade_order_detail, turn it into flink table
        tableEnv.executeSql(
                "Create table dwd_order_detail(\n" +
                                "order_detail_id string,\n" +
                                "order_id string,\n" +
                                "user_id string,\n" +
                                "sku_id string,\n" +
                                "sku_name string,\n" +
                                "province_id string,\n" +
                                "activity_id string,\n" +
                                "activity_rule_id string,\n" +
                                "coupon_id string,\n" +
                                "date_id string,\n" +
                                "create_time string,\n" +
                                "source_id string,\n" +
                                "source_type string,\n" +
                                "source_type_name string,\n" +
                                "sku_num string,\n" +
                                "split_original_amount string,\n" +
                                "split_activity_amount string,\n" +
                                "split_coupon_amount string,\n" +
                                "split_total_amount string,\n" +
                                "pt AS PROCTIME(),\n" +
                                "ts bigint,\n" +
                                "rt as TO_TIMESTAMP_LTZ(ts,0),\n" +
                                "WATERMARK FOR `rt` AS `rt` - INTERVAL '2' SECOND" +
                        ")" +
                        FlinkSqlUtil.getKafkaConsumerDDL(Common.TOPIC_DWD_TRADE_ORDER_DETAIL, Common.KAFKA_DWD_PAY_SUC_GROUP));
        //tableEnv.sqlQuery("select * from dwd_order_detail").execute().print();

        tableEnv.executeSql(FlinkSqlUtil.createTopicDBFlinkTable());
        //tableEnv.sqlQuery("select * from ods_topic_db").execute().print();

        tableEnv.createTemporarySystemFunction("getRowKey", GetRowKeyUDF.class);
        Table paymentInfo = tableEnv.sqlQuery(
                "select\n" +
                        "data['user_id'] user_id,\n" +
                        "data['order_id'] order_id,\n" +
                        "getRowKey(data['payment_type'],'dim_base_dic') payment_type,\n" +
                        "data['callback_time'] callback_time,\n" +
                        "TO_TIMESTAMP_LTZ(unix_timestamp(data['callback_time']),0) rt,\n" +
                        "pt\n" +
                        "from ods_topic_db\n" +
                        "where `table` = 'payment_info'\n" +
                        "and `type` = 'update'\n" +
                        "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_info", paymentInfo);
        //tableEnv.sqlQuery("select * from payment_info").execute().print();

        //2.read base_dic from hbase
        tableEnv.executeSql("CREATE TABLE dim_base_dic (\n" +
                "    rowkey string,\n" +
                "    info ROW<dic_name string>\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-2.2',\n" +
                "    'table-name' = 'gmall_flink:dim_base_dic',\n" +
                "    'zookeeper.quorum' = 'hadoop102:2181'\n" +
                ")");
        //tableEnv.sqlQuery("select * from dim_base_dic").execute().print();

        //3.lookup join
        Table joinTable = tableEnv.sqlQuery("" +
                "select\n" +
                    "pi.order_id order_id,\n" +
                    "pi.payment_type payment_type_code,\n" +
                    "dim_base_dic.info.dic_name payment_type_name,\n" +
                    "pi.callback_time,\n" +
                    "pi.rt\n" +
                "from payment_info pi\n" +
                "join dim_base_dic for system_time as of pi.pt\n" +
                "on pi.payment_type = dim_base_dic.rowkey");
        tableEnv.createTemporaryView("payment_info_with_name", joinTable);
        //tableEnv.sqlQuery("select * from payment_info_with_name").execute().print();

        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                    "od.order_detail_id,\n" +
                    "od.order_id,\n" +
                    "od.user_id,\n" +
                    "od.sku_id,\n" +
                    "od.sku_name,\n" +
                    "od.province_id,\n" +
                    "od.activity_id,\n" +
                    "od.activity_rule_id,\n" +
                    "od.coupon_id,\n" +
                    "pi.payment_type_code,\n" +
                    "pi.payment_type_name,\n" +
                    "pi.callback_time,\n" +
                    "od.source_id,\n" +
                    "od.source_type source_type_code,\n" +
                    "od.source_type_name,\n" +
                    "od.sku_num,\n" +
                    "od.split_original_amount,\n" +
                    "od.split_activity_amount,\n" +
                    "od.split_coupon_amount,\n" +
                    "od.split_total_amount split_payment_amount,\n" +
                    "UNIX_TIMESTAMP(cast(od.rt as string), 'yyyy-MM-dd hh:mm:ss') ts\n" +
                "from dwd_order_detail od join payment_info_with_name pi\n" +
                "on od.order_id = pi.order_id\n " +
                "and od.rt >= pi.rt - INTERVAL '15' MINUTE \n" +
                "and od.rt <= pi.rt + INTERVAL '5' SECOND");
        tableEnv.createTemporaryView("result_table", resultTable);
        //tableEnv.sqlQuery("select * from result_table").execute().print();

        tableEnv.executeSql(
                "Create table dwd_trade_pay_detail_suc(\n" +
                                "order_detail_id string,\n" +
                                "order_id string,\n" +
                                "user_id string,\n" +
                                "sku_id string,\n" +
                                "sku_name string,\n" +
                                "province_id string,\n" +
                                "activity_id string,\n" +
                                "activity_rule_id string,\n" +
                                "coupon_id string,\n" +
                                "payment_type_code string,\n" +
                                "payment_type_name string,\n" +
                                "callback_time string,\n" +
                                "source_id string,\n" +
                                "source_type_code string,\n" +
                                "source_type_name string,\n" +
                                "sku_num string,\n" +
                                "split_original_amount string,\n" +
                                "split_activity_amount string,\n" +
                                "split_coupon_amount string,\n" +
                                "split_payment_amount string,\n" +
                                "ts bigint,\n" +
                                "primary key(order_detail_id) not enforced\n" +
                        ")" + FlinkSqlUtil.getUpsertKafkaProducerDDL(Common.TOPIC_DWD_TRADE_PAY_DETAIL_SUC));


        tableEnv.executeSql("insert into dwd_trade_pay_detail_suc select * from result_table");

    }
}
