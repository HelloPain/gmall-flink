package app.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.Common;
import util.FlinkSqlUtil;
import util.GetRowKeyUDF;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/6 15:32
 * @Function: Read topic_db data from kafka,
 *            extract `comment info` data from topic_db,
 *            read dim table base_dic from hbase,
 *            join base_dic and comment_info,
 *            sink to kafka
 * @DataLink: mock -> maxwell -> kafka(topic_db) -> flink table -> kafka(dwd_interaction_comment_info)
 */
public class DwdInterationCommentInfo {
    //维度退化,把base_dic中的数据放入comment_info中
    public static void main(String[] args) throws Exception {
        //1.从kafka读取comment_info的数据，为了使用flinksql创建动态表
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(FlinkSqlUtil.createTopicDBFlinkTable());
        //tableEnv.sqlQuery("select * from ods_topic_db").execute().print();

        tableEnv.createTemporarySystemFunction("getRowKey", GetRowKeyUDF.class);
        Table commentInfo = tableEnv.sqlQuery(
                "select\n" +
                        "    `data`['id'] id,\n" +
                        "    `data`['user_id'] user_id,\n" +
                        "    `data`['nick_name'] nick_name,\n" +
                        "    `data`['sku_id'] sku_id,\n" +
                        "    `data`['spu_id'] spu_id,\n" +
                        "    `data`['order_id'] order_id,\n" +
                        "    getRowKey(`data`['appraise'],'dim_base_dic') appraise,\n" +
                        "    `data`['comment_txt'] comment_txt,\n" +
                        "    `data`['create_time'] create_time,\n" +
                        "  `pt`\n" +//这里+rt没法lookupjoin?
                        "from ods_topic_db\n" +
                        "where `database` = 'gmall_flink' " +
                        "and `table` = 'comment_info' " +
                        "and (`type` = 'insert' or `type` = 'update');\n");
        tableEnv.createTemporaryView("comment_info", commentInfo);
        //tableEnv.sqlQuery("select * from comment_info").execute().print();

        //2.从hbase读取base_dic
        tableEnv.executeSql("CREATE TABLE dim_base_dic (\n" +
                "    rowkey string,\n" +
                "    info ROW<dic_name string>\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-2.2',\n" +
                "    'table-name' = 'gmall_flink:dim_base_dic',\n" +
                "    'zookeeper.quorum' = 'hadoop102:2181'\n" +
                ")");

        //tableEnv.sqlQuery("select rowkey,SUBSTR(rowkey,4,CHAR_LENGTH(rowkey)) from dim_base_dic").execute().print();

        //3.lookup join
        Table resultTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    ci.id,\n" +
                "    ci.user_id,\n" +
                "    ci.nick_name,\n" +
                "    ci.sku_id,\n" +
                "    ci.spu_id,\n" +
                "    ci.order_id,\n" +
                "    ci.appraise,\n" +
                "    dim_base_dic.info.dic_name appraise_name,\n" +
                "    ci.comment_txt,\n" +
                "    ci.create_time\n" +
                "from comment_info ci\n" +
                "join dim_base_dic FOR SYSTEM_TIME AS OF ci.pt\n" +
                "on ci.appraise = dim_base_dic.rowkey");
        tableEnv.createTemporaryView("result_table", resultTable);
        //tableEnv.sqlQuery("select * from result_table").execute().print();

        //4.write to kafka
        tableEnv.executeSql("" +
                "create table dwd_comment_info(\n" +
                "    `id` string,\n" +
                "    `user_id` string,\n" +
                "    `nick_name` string,\n" +
                "    `sku_id` string,\n" +
                "    `spu_id` string,\n" +
                "    `order_id` string,\n" +
                "    `appraise` string,\n" +
                "    `appraise_name` string,\n" +
                "    `comment_txt` string,\n" +
                "    `create_time` string\n" +
                ")\n" + FlinkSqlUtil.getKafkaProducerDDL(Common.TOPIC_DWD_INTERACTION_COMMENT_INF));
        tableEnv.executeSql("insert into dwd_comment_info select * from result_table")
                .print();

        env.execute();
    }
}
