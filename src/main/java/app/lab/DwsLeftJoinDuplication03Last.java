package app.lab;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import util.Common;
import util.KafkaUtil;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/13 13:54
 * @Function: left join 去重 （下游需要累加）
 */
public class DwsLeftJoinDuplication03Last {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());
        final long timerDelay = 5000L;

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(Common.TOPIC_DWD_TRADE_ORDER_DETAIL, Common.KAFKA_DWD_TRADE_ORDER_DETAIL_GROUP);
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSONObject.parseObject(value);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                })
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("order_detail_id");
                    }
                })
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<JSONObject> lastRecord;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastRecord = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("timerRegistered", JSONObject.class));
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        if (lastRecord.value() == null) {
                            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + timerDelay);
                        }
                        lastRecord.update(value);
                    }

                    @Override
                    public void onTimer(long timestamp,
                                        KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx,
                                        Collector<JSONObject> out) throws Exception {
                        out.collect(lastRecord.value());
                        lastRecord.clear();
                    }
                })
                .print();

        env.execute();
    }
}
