package app.dwd;

import app.dwd.func.DwdTableProcessFunction;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.Common;
import util.KafkaUtil;

import javax.annotation.Nullable;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/8 10:20
 * @Function: filter dwd mysql data into kafka topic according to config table in mysql
 * @DataLink: mock -> maxwell -> kafka + mysql -> flinkApp -> kafka topic
 * (dwd_interaction_favor_add
 * dwd_tool_coupon_get
 * dwd_tool_coupon_use
 * dwd_user_register)
 */
public class DwdBaseApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        //1.read topic_db data from kafka
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(Common.TOPIC_ODS_DB, Common.KAFKA_DIM_GROUP);
        DataStreamSource<String> kafkaDs = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(), "kafkaSource");
        SingleOutputStreamOperator<JSONObject> kafkaJsonDs =
                kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                });
       // kafkaJsonDs.print("kafkaJsonDs>>");

        //2.read dwd config data from mysql
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(Common.MYSQL_HOST)
                .port(Common.MYSQL_PORT)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .username(Common.MYSQL_USERNAME)
                .password(Common.MYSQL_PASSWORD)
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        SingleOutputStreamOperator<TableProcess> configDs =
                env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource")
                .map(new MapFunction<String, TableProcess>() {
                    @Override
                    public TableProcess map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        String op = obj.getString("op");
                        TableProcess res;
                        if ("d".equals(op)) {
                            res = JSON.parseObject(obj.getString("before"), TableProcess.class);
                        } else {
                            res = JSON.parseObject(obj.getString("after"), TableProcess.class);
                        }
                        res.setOp(op);
                        return res;
                    }
                });
        //configDs.print("configDs>>");

        //3.broadcast and filter kafkaJsonDs according to configDs
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("dwd-config", String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcastStream = configDs.broadcast(mapStateDescriptor);
        SingleOutputStreamOperator<JSONObject> filteredDs = kafkaJsonDs.connect(broadcastStream)
                .process(new DwdTableProcessFunction(mapStateDescriptor));
        //filteredDs.print("filteredDs>>");

        //4.sink to kafka
        filteredDs.sinkTo(KafkaUtil.getKafkaSink(new KafkaRecordSerializationSchema<JSONObject>() {
            @Nullable
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, KafkaSinkContext kafkaSinkContext, Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sink_topic"), jsonObject.toJSONString().getBytes());
            }
        }));

        env.execute();
    }
}
