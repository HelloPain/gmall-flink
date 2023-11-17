package app.dws;


import bean.TradePaymentBean;
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
import util.*;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/16 15:00
 * @Function: 从Kafka读取交易域支付成功主题数据，统计当日支付成功独立用户数和首次（全局）支付成功用户数（新付费用户）。
 */
public class DwsTradePaymentWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(
                Common.TOPIC_DWD_TRADE_PAY_DETAIL_SUC,
                Common.KAFKA_DWD_TRADE_PAY_DETAIL_SUC_GROUP);
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
                                (element, recordTimestamp) -> element.getLong("ts") * 1000))
                //1. key by user id and duplication
                .keyBy(jsonObj -> jsonObj.getString("user_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, TradePaymentBean>() {
                    ValueState<String> curDate;     //the date of current user data

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        curDate = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("curDate", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradePaymentBean> out) throws Exception {
                        String date = DateFormatUtil.toDate(value.getLong("ts") * 1000);
                        if (curDate.value() == null || !curDate.value().equals(date)) {
                            long uv;
                            long newUv = 0L;
                            if (curDate.value() == null) { //first come
                                newUv = 1L;
                            }
                            curDate.update(date);
                            uv = 1L;
                            out.collect(new TradePaymentBean(
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
                .keyBy(new KeySelector<TradePaymentBean, String>() {
                    @Override
                    public String getKey(TradePaymentBean value) throws Exception {
                        return value.getCurDate();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(Common.WINDOW_SIZE_SECONDS)))
                .reduce(new ReduceFunction<TradePaymentBean>() {
                    @Override
                    public TradePaymentBean reduce(TradePaymentBean value1, TradePaymentBean value2) throws Exception {
                        value1.setPaymentSucNewUserCt(value1.getPaymentSucNewUserCt() + value2.getPaymentSucNewUserCt());
                        value1.setPaymentSucUniqueUserCt(value1.getPaymentSucUniqueUserCt() + value2.getPaymentSucUniqueUserCt());
                        return value1;
                    }
                }, new WindowFunction<TradePaymentBean, TradePaymentBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradePaymentBean> input, Collector<TradePaymentBean> out) throws Exception {
                        WindowUtil.<TradePaymentBean>addWindowInfo(window, input, out);
                    }
                })
                .map(new MapFunction<TradePaymentBean, String>() {
                    @Override
                    public String map(TradePaymentBean value) throws Exception {
                        SerializeConfig config = new SerializeConfig();
                        config.setPropertyNamingStrategy(com.alibaba.fastjson.PropertyNamingStrategy.SnakeCase);
                        return JSONObject.toJSONString(value, config);
                    }
                })
                .sinkTo(DorisUtil.getDorisSink("gmall_flink.dws_trade_payment_suc_window"));
        //.print();

        env.execute();
    }
}
