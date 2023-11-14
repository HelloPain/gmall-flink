package app.dws;

import bean.TradeOrderBean;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


import org.apache.flink.util.Collector;
import util.Common;
import util.DorisUtil;
import util.KafkaUtil;
import util.WindowUtil;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/13 14:46
 * @Function: 从 Kafka订单明细主题读取数据，统计当日下单独立用户数和首次下单用户数，封装为实体类，写入Doris。
 */
public class DwsTradeOrderWindow {
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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>)
                                (element, recordTimestamp) -> element.getLong("ts")))
                //1. key by user id and duplication
                .keyBy(jsonObj -> jsonObj.getString("user_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {
                    ValueState<String> curDate;     //the date of current user data

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        curDate = getRuntimeContext().getState(new ValueStateDescriptor<String>("curDate", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradeOrderBean> out) throws Exception {
                        String date = value.getString("create_time").split(" ")[0];

                        if (curDate.value() == null || !curDate.value().equals(date)) {
                            long uv;
                            long newUv = 0L;
                            if (curDate.value() == null) {
                                newUv = 1L;
                            }
                            curDate.update(date);
                            uv = 1L;
                            out.collect(new TradeOrderBean(
                                    "",
                                    "",
                                    date,
                                    uv,
                                    newUv
                            ));
                        }
                    }
                })
                //2. key by window reduce
                .keyBy(new KeySelector<TradeOrderBean, String>() {
                    @Override
                    public String getKey(TradeOrderBean value) throws Exception {
                        return value.getCurDate();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(Common.WINDOW_SIZE_SECONDS)))
                //.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                                return value1;
                            }
                        }, new WindowFunction<TradeOrderBean, TradeOrderBean, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<TradeOrderBean> input, Collector<TradeOrderBean> out) throws Exception {
                                WindowUtil.<TradeOrderBean>addWindowInfo(window, input, out);
                            }
                        })
//                        new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
//                            @Override
//                            public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
//                                WindowUtil.<TradeOrderBean>addWindowInfo(window, values, out);
//                            }

                .map(new MapFunction<TradeOrderBean, String>() {
                    @Override
                    public String map(TradeOrderBean value) throws Exception {
                        SerializeConfig config = new SerializeConfig();
                        config.setPropertyNamingStrategy(com.alibaba.fastjson.PropertyNamingStrategy.SnakeCase);
                        return JSONObject.toJSONString(value, config);
                    }
                })
                .sinkTo(DorisUtil.getDorisSink("gmall_flink.dws_trade_order_window"));
               // .print();

        env.execute();
    }
}
