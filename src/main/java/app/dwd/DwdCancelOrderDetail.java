package app.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.Common;
import util.FlinkSqlUtil;

import java.time.Duration;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/7 10:46
 * @Function: 从 Kafka 读取topic_db主题数据，
 *            关联筛选订单明细表、取消订单数据、订单明细活动关联表、订单明细优惠券关联表四张事实业务表形成取消订单明细表，
 *            写入 Kafka 对应主题。
 *            Read topic_db from kafka,
 *            Extract `order_info` (filter cancel order),`order_detail`,`order_detail_activity`,`order_detail_coupon` data from topic_db, join them,
 *            Sink to kafka
 * @DataLink: mock -> maxwell -> kafka(topic_db) -> flink table -> kafka(dwd_trade_cancel_detail)
 */
public class DwdCancelOrderDetail {
    public static void main(String[] args) throws Exception {
        //1.read from kafka topic_db, turn it into flink table
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(FlinkSqlUtil.createTopicDBFlinkTable());
        //tableEnv.sqlQuery("select * from ods_topic_db").execute().print();

        //set TTL for join
        TableConfig config = tableEnv.getConfig();
        config.setIdleStateRetention(Duration.ofSeconds(10));

        //2.get canceled order info from topic_db
        Table orderInfo = tableEnv.sqlQuery(
                   "select\n" +
                   "    `data`['id'] id,  \n" +
                   "    `data`['consignee'] consignee,  \n" +
                   "    `data`['consignee_tel'] consignee_tel,  \n" +
                   "    `data`['total_amount'] total_amount,  \n" +
                   "    `data`['order_status'] order_status,  \n" +
                   "    `data`['user_id'] user_id,  \n" +
                   "    `data`['payment_way'] payment_way,  \n" +
                   "    `data`['delivery_address'] delivery_address,  \n" +
                   "    `data`['order_comment'] order_comment,  \n" +
                   "    `data`['out_trade_no'] out_trade_no,  \n" +
                   "    `data`['trade_body'] trade_body,  \n" +
                   "    `data`['create_time'] create_time,  \n" +
                   "    `data`['operate_time'] operate_time,  \n" +
                   "    `data`['expire_time'] expire_time,  \n" +
                   "    `data`['process_status'] process_status,  \n" +
                   "    `data`['tracking_no'] tracking_no,  \n" +
                   "    `data`['parent_order_id'] parent_order_id,  \n" +
                   "    `data`['province_id'] province_id,  \n" +
                   "    `data`['activity_reduce_amount'] activity_reduce_amount,  \n" +
                   "    `data`['coupon_reduce_amount'] coupon_reduce_amount,  \n" +
                   "    `data`['original_total_amount'] original_total_amount,  \n" +
                   "    `data`['refundable_time'] refundable_time\n" +
                   "from ods_topic_db\n" +
                   "where `database` = 'gmall_flink'\n" +
                   "      and `table` = 'order_info'\n" +
                   "      and `type` = 'update'\n" +
                   "      and `old`['order_status'] is not null\n" +
                   "      and `data`['order_status'] = '1003';");
        tableEnv.createTemporaryView("order_cancel_info", orderInfo);
        //tableEnv.sqlQuery("select * from order_cancel_info").execute().print();

        Table orderDetail = tableEnv.sqlQuery(
                "select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['order_id'] order_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['sku_name'] sku_name,\n" +
                "    `data`['img_url'] img_url,\n" +
                "    `data`['order_price'] order_price,\n" +
                "    `data`['sku_num'] sku_num,\n" +
                "    `data`['create_time'] create_time,\n" +
                "    `data`['source_type'] source_type,\n" +
                "    `data`['source_id'] source_id,\n" +
                "    `data`['split_total_amount'] split_total_amount,\n" +
                "    `data`['split_activity_amount'] split_activity_amount,\n" +
                "    `data`['split_coupon_amount'] split_coupon_amount,\n" +
                "    `data`['operate_time'] operate_time\n" +
                "from ods_topic_db\n" +
                "where `database` = 'gmall_flink'\n" +
                "      and `table` = 'order_detail'\n" +
                "      and `type` = 'insert';");
        tableEnv.createTemporaryView("order_detail", orderDetail);
        //tableEnv.sqlQuery("select * from order_detail").execute().print();

        Table orderDetailActivity = tableEnv.sqlQuery(
                "select\n" +
                "  `data`['id'] id,\n" +
                "  `data`['order_id'] order_id,\n" +
                "  `data`['order_detail_id'] order_detail_id,\n" +
                "  `data`['activity_id'] activity_id,\n" +
                "  `data`['activity_rule_id'] activity_rule_id,\n" +
                "  `data`['sku_id'] sku_id,\n" +
                "  `data`['create_time'] create_time,\n" +
                "  `data`['operate_time'] operate_time\n" +
                "from ods_topic_db\n" +
                "where `database` = 'gmall_flink'\n" +
                "      and `table` = 'order_detail_activity'\n" +
                "      and `type` = 'insert';");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);
        //tableEnv.sqlQuery("select * from order_detail_activity").execute().print();

        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select\n" +
                "   `data`['id'] id,\n" +
                "   `data`['order_id'] order_id,\n" +
                "   `data`['order_detail_id'] order_detail_id,\n" +
                "   `data`['coupon_id'] coupon_id,\n" +
                "   `data`['coupon_use_id'] coupon_use_id,\n" +
                "   `data`['sku_id'] sku_id,\n" +
                "   `data`['create_time'] create_time,\n" +
                "   `data`['operate_time'] operate_time\n" +
                "from ods_topic_db\n" +
                "where `database` = 'gmall_flink'\n" +
                "      and `table` = 'order_detail_coupon'\n" +
                "      and `type` = 'insert';");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
        //tableEnv.sqlQuery("select * from order_detail_coupon").execute().print();

        //3.join
        Table joined = tableEnv.sqlQuery(
                "select\n" +
                "      od.id order_detail_id,\n" +
                "      oi.id order_id,\n" +
                "      oi.consignee,\n" +
                "      oi.consignee_tel,\n" +
                "      oi.total_amount,\n" +
                "      oi.order_status,\n" +
                "      oi.user_id,\n" +
                "      oi.payment_way,\n" +
                "      oi.delivery_address,\n" +
                "      oi.order_comment,\n" +
                "      oi.out_trade_no,\n" +
                "      oi.trade_body,\n" +
                "      oi.create_time,\n" +
                "      oi.process_status,\n" +
                "      oi.tracking_no,\n" +
                "      oi.parent_order_id,\n" +
                "      oi.province_id,\n" +
                "      oi.activity_reduce_amount,\n" +
                "      oi.coupon_reduce_amount,\n" +
                "      oi.original_total_amount,\n" +
                "      oi.refundable_time,\n" +
                "\n" +
                "      od.sku_id,\n" +
                "      od.sku_name,\n" +
                "      od.img_url,\n" +
                "      od.order_price,\n" +
                "      od.sku_num,\n" +
                "      od.source_type,\n" +
                "      od.source_id,\n" +
                "      od.split_total_amount,\n" +
                "      od.split_activity_amount,\n" +
                "      od.split_coupon_amount,\n" +
                "\n" +
                "      oa.id order_detail_activity_id,\n" +
                "      oa.activity_id,\n" +
                "      oa.activity_rule_id,\n" +
                "\n" +
                "      oc.id order_detail_coupon_id,\n" +
                "      oc.coupon_id,\n" +
                "      oc.coupon_use_id\n" +
                "\n" +
                "from \n" +
                "      order_cancel_info oi join order_detail od on oi.id = od.order_id\n" +
                "      left join order_detail_activity oa on od.id = oa.order_detail_id\n" +
                "      left join order_detail_coupon oc on od.id = oc.order_detail_id");
        tableEnv.createTemporaryView("joined", joined);
        //tableEnv.sqlQuery("select * from joined").execute().print();

        //4.sink to kafka
        tableEnv.executeSql(
                "create table dwd_cancel_order_detail(\n" +
                "      order_detail_id string,\n" +
                "      order_id string,\n" +
                "      consignee string,\n" +
                "      consignee_tel string,\n" +
                "      total_amount string,\n" +
                "      order_status string,\n" +
                "      user_id string,\n" +
                "      payment_way string,\n" +
                "      delivery_address string,\n" +
                "      order_comment string,\n" +
                "      out_trade_no string,\n" +
                "      trade_body string,\n" +
                "      create_time string,\n" +
                "      process_status string,\n" +
                "      tracking_no string,\n" +
                "      parent_order_id string,\n" +
                "      province_id string,\n" +
                "      activity_reduce_amount string,\n" +
                "      coupon_reduce_amount string,\n" +
                "      original_total_amount string,\n" +
                "      refundable_time string,\n" +
                "      sku_id string,\n" +
                "      sku_name string,\n" +
                "      img_url string,\n" +
                "      order_price string,\n" +
                "      sku_num string,\n" +
                "      source_type string,\n" +
                "      source_id string,\n" +
                "      split_total_amount string,\n" +
                "      split_activity_amount string,\n" +
                "      split_coupon_amount string,\n" +
                "      order_detail_activity_id string,\n" +
                "      activity_id string,\n" +
                "      activity_rule_id string,\n" +
                "      order_detail_coupon_id string,\n" +
                "      coupon_id string,\n" +
                "      coupon_use_id string,\n" +
                "    PRIMARY KEY (`order_detail_id`) NOT ENFORCED\n" +
                ")" + FlinkSqlUtil.getUpsertKafkaProducerDDL(Common.TOPIC_DWD_TRADE_CANCEL_DETAIL));

        tableEnv.executeSql("insert into dwd_cancel_order_detail select * from joined");

        //env.execute();
    }
}
