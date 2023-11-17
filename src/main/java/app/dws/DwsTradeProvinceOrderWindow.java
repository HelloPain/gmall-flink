package app.dws;

import app.dws.func.AddDimInfoAsyncFunc;
import bean.TradeProvinceOrderBean;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.*;

import java.util.concurrent.TimeUnit;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/17 9:46
 * @Function: 从 Kafka 读取订单明细数据，过滤 null 数据并按照唯一键对数据去重，统计各省份各窗口订单数和订单金额，将数据写入Doris 交易域省份粒度下单各窗口汇总表。
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(Common.TOPIC_DWD_TRADE_ORDER_DETAIL, Common.KAFKA_DWD_TRADE_ORDER_DETAIL_GROUP);
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedDs =
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
                        //duplication for order id
                        .keyBy(new KeySelector<JSONObject, String>() {
                            @Override
                            public String getKey(JSONObject value) throws Exception {
                                return value.getString("order_id");
                            }
                        })
                        .flatMap(new RichFlatMapFunction<JSONObject, TradeProvinceOrderBean>() {

                            ValueState<Boolean> hasState; //is this order id existed already

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                hasState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("has_state", Boolean.class));
                            }

                            @Override
                            public void flatMap(JSONObject value, Collector<TradeProvinceOrderBean> out) throws Exception {
                                if (hasState.value() == null) {
                                    hasState.update(true);
                                    out.collect(new TradeProvinceOrderBean(
                                            "",
                                            "",
                                            value.getString("province_id"),
                                            "",
                                            value.getString("order_id"),
                                            DateFormatUtil.toDate(value.getLong("ts") * 1000),
                                            1L,
                                            value.getBigDecimal("original_total_amount"),
                                            value.getLong("ts")
                                    ));
                                }
                            }
                        })
                        .keyBy(new KeySelector<TradeProvinceOrderBean, String>() {
                            @Override
                            public String getKey(TradeProvinceOrderBean value) throws Exception {
                                return value.getProvinceId();
                            }
                        })
                        .window(TumblingEventTimeWindows.of(Time.seconds(Common.WINDOW_SIZE_SECONDS)))
                        .reduce((value1, value2) -> {
                            value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                            value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                            return value1;
                        }, new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                                WindowUtil.<TradeProvinceOrderBean>addWindowInfo(window, input, out);
                            }
                        });

        int asyncTimeout = 100;//longer than connection delay
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedDsWithProName =
                AsyncDataStream.unorderedWait(reducedDs,
                        new AddDimInfoAsyncFunc<TradeProvinceOrderBean, TradeProvinceOrderBean>("dim_base_province") {
                            @Override
                            public String getPk(TradeProvinceOrderBean input) {
                                return input.getProvinceId();
                            }

                            @Override
                            public TradeProvinceOrderBean join(TradeProvinceOrderBean value, JSONObject dimInfo) {
                                value.setProvinceName(dimInfo.getString("name"));
                                return value;
                            }
                        }, asyncTimeout, TimeUnit.SECONDS);
        //reducedDsWithProName.print();
        reducedDsWithProName.map(new MapFunction<TradeProvinceOrderBean, String>() {
                    @Override
                    public String map(TradeProvinceOrderBean value) throws Exception {
                        SerializeConfig serializeConfig = new SerializeConfig();
                        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
                        return JSONObject.toJSONString(value, serializeConfig);
                    }
                })
                .sinkTo(DorisUtil.getDorisSink("gmall_flink.dws_trade_province_order_window"));
        env.execute();
    }
}
