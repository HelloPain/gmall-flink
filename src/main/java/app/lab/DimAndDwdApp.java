package app.lab;

import app.dim.func.DimCreateTableMapFunction;
import app.dim.func.DimSinkFunction;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.Common;
import util.KafkaUtil;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/10 8:53
 * @Function: Filter dwd mysql data into kafka topic according to config table in mysql,
 *            Put dim table input hbase according to config table in mysql
 * @DataLink: mock -> maxwell -> kafka + mysql -> flinkApp -> kafka topic + hbase
 */
public class DimAndDwdApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);//=kafka partition

        System.setProperty("HADOOP_USER_NAME", "pj");
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(30000L);
        checkpointConfig.setCheckpointStorage(Common.CHECKPOINT_PATH);
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        checkpointConfig.setMinPauseBetweenCheckpoints(10000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, 2000L));
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(3), CheckpointingMode.EXACTLY_ONCE);

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
                            String type = jsonObj.getString("type");
                            if (!"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                });
        kafkaDs.print("kafkaDs>>");

        //2.read dim hbase config data from mysql
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(Common.MYSQL_HOST)
                .port(Common.MYSQL_PORT)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .username("root")
                .password("000000")
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlDs = env.fromSource(mysqlSource,
                WatermarkStrategy.noWatermarks(), "mysqlSource");
        mysqlDs.print("mysqlDs>>");

        //K: sourceTable, V: TableProcess
        MapStateDescriptor<String, TableProcess> mapStateDescriptor =
                new MapStateDescriptor<>("mapState", String.class, TableProcess.class);

        BroadcastStream<TableProcess> broadcastMysqlDs = mysqlDs
                .map(new DimCreateTableMapFunction())
                .broadcast(mapStateDescriptor);

        OutputTag<JSONObject> dwdOutputTag = new OutputTag<JSONObject>("dwdOutputTag"){};
        SingleOutputStreamOperator<JSONObject> dimDs = kafkaJsonDs.connect(broadcastMysqlDs)
                .process(new DimAndDwdTableProcessFunction(mapStateDescriptor,dwdOutputTag));

        dimDs.print("hbaseDS>>");
        dimDs.addSink(new DimSinkFunction());
        dimDs.getSideOutput(dwdOutputTag)
                .sinkTo(KafkaUtil.getKafkaSink(new KafkaRecordSerializationSchema<JSONObject>() {
            @Nullable
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, KafkaSinkContext kafkaSinkContext, Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sink_topic"), jsonObject.toJSONString().getBytes());
            }
        }));

        env.execute("Dim and dwd base app");
    }
}
