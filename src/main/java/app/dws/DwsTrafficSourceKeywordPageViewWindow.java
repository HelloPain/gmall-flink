package app.dws;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import util.SplitKeywordUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import util.Common;
import util.FlinkSqlUtil;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/10 10:32
 * @Function: 流量域 来源关键词粒度 页面浏览 各窗口轻度聚合
 * @DataLink: mock -> flume -> kafka -> flink(page split) ->
 *                  kafka(dwd_traffic_page) -> flink -> doris(dws_traffic_source_keyword_page_view_window)
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //checkpoint must be set because doris sink is 2pc
        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());

        /*
        * {"common":{"ar":"34","uid":"375","os":"Android 12.0","ch":"oppo","is_new":"1","md":"Redmi k50","mid":"mid_57","vc":"v2.1.134","ba":"Redmi","sid":"cb4e5c0a-1b0e-4f99-ade3-387247e6ec32"},
        * "page":{"page_id":"payment","item":"56603","during_time":18825,"item_type":"order_id","last_page_id":"trade"},
        * "ts":1677770436979}
         * */
        tableEnv.executeSql("create table page_log(\n" +
                "  `page` map<string,string>,\n" +
                "  `ts` bigint,\n" +
                "  `rt` as to_timestamp_ltz(`ts`,3),\n" +
                "  watermark for `rt` as `rt` - INTERVAL '2' SECONDS\n" +
                ")" + FlinkSqlUtil.getKafkaConsumerDDL(Common.TOPIC_DWD_TRAFFIC_PAGE, Common.KAFKA_DWS_PAGE_GROUP));
        //tableEnv.sqlQuery("select * from page_log").execute().print();

        Table filterTable = tableEnv.sqlQuery("" +
                "select\n" +
                "    `page`['item'] item,\n" +
                "    `rt`\n" +
                "from page_log\n" +
                "where `page`['last_page_id'] = 'search'\n" +
                "and `page`['item'] is not null\n" +
                "and `page`['item_type'] = 'keyword'");
        tableEnv.createTemporaryView("filter_table", filterTable);
        //tableEnv.sqlQuery("select * from filter_table").execute().print();

        tableEnv.createTemporarySystemFunction("split_func", SplitKeywordUDTF.class);
        Table splitTable = tableEnv.sqlQuery("select \n" +
                "  word, \n" + //TODO:为什么这里不能写keyword？
                "  rt\n" +
                "from filter_table, lateral table(split_func(item))");
        tableEnv.createTemporaryView("split_table", splitTable);
        //tableEnv.sqlQuery("select * from split_table").execute().print();

        Table winAggTable = tableEnv.sqlQuery("select \n" +
                "  date_format(window_start,'yyyy-MM-dd HH:mm:ss') as stt,\n" +
                "  date_format(window_end,'yyyy-MM-dd HH:mm:ss') as edt,\n" +
                "  word as keyword,\n" +
                "  date_format(window_start,'yyyy-MM-dd') as curdt,\n" +
                "  count(*) as keyword_count\n" +
                "from table(\n" +
                "  TUMBLE(\n" +
                "    DATA => table split_table,\n" +
                "    TIMECOL => DESCRIPTOR(rt),\n" +
                "    SIZE => INTERVAL '10' SECONDS\n" +
                "  )\n" +
                ")\n" +
                "GROUP by window_start, window_end, word ");
        tableEnv.createTemporaryView("win_table", winAggTable);
        //tableEnv.sqlQuery("select * from win_table").execute().print();
        //tableEnv.toDataStream(winAggTable).print();

        tableEnv.executeSql(
                "CREATE table doris_t(  " +
                " stt string, " +
                " edt string, " +
                " keyword string, " +
                " cur_date string, " +
                " keyword_count bigint " +
                ")WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = 'hadoop102:7030', " +
                "  'table.identifier' = 'gmall_flink.dws_traffic_source_keyword_page_view_window', " +
                "  'username' = 'root', " +
                "  'password' = '', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.properties.read_json_by_line' = 'true', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false' " + // 测试阶段可以关闭两阶段提交,方便测试
                ")");
        winAggTable.insertInto("doris_t")
                .execute()
                .print();

        env.execute("TrafficSourceKeywordPageViewWindow");
    }
}
