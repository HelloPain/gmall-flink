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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import util.Common;
import util.KafkaUtil;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/13 13:54
 * @Function: left join 去重 （下游需要累加）
 */
public class DwsLeftJoinDuplication02First {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());

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
                .filter(new RichFilterFunction<JSONObject>() {
                    private ValueState<Boolean> hasState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> stateDescriptor = new ValueStateDescriptor<>("value", Boolean.class);
//                        stateDescriptor.enableTimeToLive(new StateTtlConfig.Builder(Time.seconds(5))
//                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
//                                .build());
                        hasState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        if (hasState.value() == null) {
                            hasState.update(true);
                            return true;
                        } else {
                            return false;
                        }
                    }
                }).print();

        env.execute();
    }
}
