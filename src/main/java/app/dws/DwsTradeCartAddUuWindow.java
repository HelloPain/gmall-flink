package app.dws;

import bean.CartAddUuBean;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.Common;
import util.DorisUtil;
import util.KafkaUtil;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import util.WindowUtil;



/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/13 21:00
 * @Function: 从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 Doris。
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        //checkpoint must be set because doris sink is 2pc
        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());

        DataStreamSource<String> kafkaDs = env.fromSource(KafkaUtil.getKafkaSource(
                        Common.TOPIC_DWD_TRADE_CART_ADD, Common.KAFKA_DWD_CARD_ADD_GROUP),
                WatermarkStrategy.noWatermarks(), "kafkaSource");
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000L));

        kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>)
                                (element, recordTimestamp) -> {
                                    //maybe update, then ws should be operate_time
                                    Long operateTime = element.getLong("operate_time");
                                    if (operateTime != null) {
                                        return operateTime;
                                    }
                                    //maybe insert, ws should be create_time
                                    return element.getLong("create_time");
                                }))
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("user_id");
                    }
                })
                .flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
                    ValueState<String> curDate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> desc = new ValueStateDescriptor<String>("curDate", String.class);
                        desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        curDate = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                        String operateTime = value.getString("operate_time");
                        String date;
                        if (operateTime != null) {
                            //maybe update, then ws should be operate_time
                            date = operateTime.split(" ")[0];
                        } else {
                            //maybe insert, ws should be create_time
                            date = value.getString("create_time").split(" ")[0];
                        }
                        if (curDate.value() == null || !curDate.value().equals(date)) {
                            curDate.update(date);
                            out.collect(new CartAddUuBean(
                                    "",
                                    "",
                                    date,
                                    1L
                            ));
                        }
                    }
                })
                .keyBy(new KeySelector<CartAddUuBean, String>() {
                    @Override
                    public String getKey(CartAddUuBean value) throws Exception {
                        return value.getCurDate();
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(Common.WINDOW_SIZE_SECONDS)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new WindowFunction<CartAddUuBean, CartAddUuBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<CartAddUuBean> input, Collector<CartAddUuBean> out) throws Exception {
                        WindowUtil.addWindowInfo(window, input, out);
                    }
                })
                .map(new MapFunction<CartAddUuBean, String>() {
                    @Override
                    public String map(CartAddUuBean value) throws Exception {
                        SerializeConfig config = new SerializeConfig();
                        config.setPropertyNamingStrategy(com.alibaba.fastjson.PropertyNamingStrategy.SnakeCase);
                        return JSONObject.toJSONString(value, config);
                    }
                })
                .sinkTo(DorisUtil.getDorisSink("gmall_flink.dws_trade_cart_add_uu_window"));

        env.execute();
    }
}
