package app.lab;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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
public class DwsLeftJoinDuplication01Sum {
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
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    ValueState<Boolean> leftState = null;
                    ValueState<Boolean> rightState1 = null;
                    ValueState<Boolean> rightState2 = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> leftStateDesc = new ValueStateDescriptor<Boolean>("leftState", Boolean.class);
                        leftStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());
                        leftState = getRuntimeContext().getState(leftStateDesc);

                        ValueStateDescriptor<Boolean> rightState1Desc = new ValueStateDescriptor<Boolean>("leftState", Boolean.class);
                        rightState1Desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());
                        rightState1 = getRuntimeContext().getState(rightState1Desc);

                        ValueStateDescriptor<Boolean> rightState2Desc = new ValueStateDescriptor<Boolean>("leftState", Boolean.class);
                        rightState2Desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());
                        rightState2 = getRuntimeContext().getState(rightState2Desc);

                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        Boolean left = leftState.value();
                        Boolean right1 = rightState1.value();
                        Boolean right2 = rightState2.value();
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("order_detail_id", value.getString("order_detail_id"));

                        if (left == null) {
                            jsonObject.put("order_id", value.getString("order_id"));
                            leftState.update(true);
                        } else {
                            leftState.update(false);
                        }

                        if (right1 == null) {
                            String activityId = value.getString("activity_id");
                            if (activityId != null) {
                                jsonObject.put("activity_id", activityId);
                                rightState1.update(true);
                            } else {
                                rightState1.update(false);
                            }
                        }

                        if (right2 == null) {
                            String activityId = value.getString("coupon_id");
                            if (activityId != null) {
                                jsonObject.put("coupon_id", activityId);
                                rightState1.update(true);
                            } else {
                                rightState1.update(false);
                            }
                        }
                        return jsonObject;
                    }
                }).print();

        env.execute();
    }
}
