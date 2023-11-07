package util;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/6 16:52
 */
public class FlinkSqlUtil {
    public static String createTopicDBFlinkTable(){
        return "create table ods_topic_db(\n" +
                "    `database` string,\n" +
                "    `table` string,\n" +
                "    `type` string,\n" +
                "    `ts` bigint,\n" +
                "    `data` map<string,string>,\n" +
                "    `old` map<string,string>,\n" +
                "    `pt` AS PROCTIME(),\n" +
                "    `rt` as TO_TIMESTAMP_LTZ(`ts`,0),\n" +
                "    WATERMARK FOR `rt` AS `rt` - INTERVAL '2' SECOND\n" +
                ")" +  FlinkSqlUtil.getKafkaConsumerDDL(Common.TOPIC_ODS_DB, Common.KAFKA_DWD_COMMENT_INFO_GROUP);
    }

    public static String getKafkaConsumerDDL(String topic, String groupId) {
        return "with(\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '" + topic + "',\n" +
                "    'properties.bootstrap.servers' = '" + Common.KAFKA_SERVERS + "',\n" +
                "    'properties.group.id' = '" + groupId + "',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'json'\n" +
                ")";
    }

    public static String getKafkaProducerDDL(String topic) {
        return " with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '" + topic + "',\n" +
                "    'properties.bootstrap.servers' = '" + Common.KAFKA_SERVERS + "',\n" +
                "    'format' = 'json'\n" +
                ")";
    }

    public static String getUpsertKafkaProducerDDL(String topic) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Common.KAFKA_SERVERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

}
