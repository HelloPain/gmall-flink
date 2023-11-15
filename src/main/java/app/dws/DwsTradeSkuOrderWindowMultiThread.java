package app.dws;


import app.dws.func.AddDimInfoAsyncFunc;
import app.dws.func.AddDimInfoMultiThreadFunc;
import bean.TradeSkuOrderBean;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import util.Common;
import util.KafkaUtil;
import util.WindowUtil;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/14 10:46
 * @Function:
 */
public class DwsTradeSkuOrderWindowMultiThread {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());
        int duplicationTTLSec = 5;
        int asyncTimeout = 100;//longer than connection delay

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(Common.TOPIC_DWD_TRADE_ORDER_DETAIL, Common.KAFKA_DWD_TRADE_ORDER_DETAIL_GROUP);

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDs =
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
                                (element, recordTimestamp) -> element.getLong("ts") * 1000L))
                .keyBy(jsonObj -> jsonObj.getString("order_detail_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, TradeSkuOrderBean>() {
                    ValueState<Boolean> hasState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> stateDescriptor = new ValueStateDescriptor<>("state", Boolean.class);
                        stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(
                                org.apache.flink.api.common.time.Time.seconds(duplicationTTLSec)).build());
                        hasState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradeSkuOrderBean> out) throws Exception {
                        if (hasState.value() == null) {
                            hasState.update(true);
                            BigDecimal splitTotalAmount = value.getBigDecimal("split_total_amount") == null ?
                                    BigDecimal.ZERO : value.getBigDecimal("split_total_amount");
                            BigDecimal splitActivityAmount = value.getBigDecimal("split_activity_amount") == null ?
                                    BigDecimal.ZERO : value.getBigDecimal("split_activity_amount");
                            BigDecimal splitCouponAmount = value.getBigDecimal("split_coupon_amount") == null ?
                                    BigDecimal.ZERO : value.getBigDecimal("split_coupon_amount");
                            BigDecimal splitOriginalAmount = BigDecimal.ZERO;
                            if (value.getBigDecimal("order_price") != null && value.getBigDecimal("sku_num") != null) {
                                splitOriginalAmount = value.getBigDecimal("order_price").multiply(value.getBigDecimal("sku_num"));
                            }
                            out.collect(TradeSkuOrderBean.builder()
                                    .skuId(value.getString("sku_id"))
                                    .skuName(value.getString("sku_name"))
                                    .originalAmount(splitOriginalAmount)
                                    .activityAmount(splitActivityAmount)
                                    .couponAmount(splitCouponAmount)
                                    .orderAmount(splitTotalAmount)
                                    .curDate(value.getString("create_time").split(" ")[0])
                                    .build());
                        }
                    }
                })
                .keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(Time.seconds(Common.WINDOW_SIZE_SECONDS)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.setActivityAmount(value1.getActivityAmount().add(value2.getActivityAmount()));
                                value1.setCouponAmount(value1.getCouponAmount().add(value2.getCouponAmount()));
                                return value1;
                            }
                        },
                        new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                                WindowUtil.<TradeSkuOrderBean>addWindowInfo(window, input, out);
                            }
                        });

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceWithSkuDs =
                AsyncDataStream.unorderedWait(reduceDs,
                        new AddDimInfoMultiThreadFunc<TradeSkuOrderBean, TradeSkuOrderBean>("dim_sku_info") {
                            @Override
                            public String getPk(TradeSkuOrderBean input) {
                                return input.getSkuId();
                            }

                            @Override
                            public TradeSkuOrderBean join(TradeSkuOrderBean value, JSONObject dimInfo) {
                                value.setSpuId(dimInfo.getString("spu_id"));
                                value.setTrademarkId(dimInfo.getString("tm_id"));
                                value.setCategory3Id(dimInfo.getString("category3_id"));
                                return value;
                            }
                        }, asyncTimeout, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceWithSpuDs = AsyncDataStream.unorderedWait(reduceWithSkuDs,
                new AddDimInfoMultiThreadFunc<TradeSkuOrderBean, TradeSkuOrderBean>("dim_spu_info") {
                    @Override
                    public String getPk(TradeSkuOrderBean input) {
                        return input.getSpuId();
                    }

                    @Override
                    public TradeSkuOrderBean join(TradeSkuOrderBean value, JSONObject dimInfo) {
                        value.setSpuName(dimInfo.getString("spu_name"));
                        return value;
                    }
                }, asyncTimeout, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceWithTmDs = AsyncDataStream.unorderedWait(reduceWithSpuDs,
                new AddDimInfoMultiThreadFunc<TradeSkuOrderBean, TradeSkuOrderBean>("dim_base_trademark") {
                    @Override
                    public String getPk(TradeSkuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public TradeSkuOrderBean join(TradeSkuOrderBean value, JSONObject dimInfo) {
                        value.setTrademarkName(dimInfo.getString("tm_name"));
                        return value;
                    }
                }, asyncTimeout, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceWithC3DS = AsyncDataStream.unorderedWait(reduceWithTmDs,
                new AddDimInfoMultiThreadFunc<TradeSkuOrderBean, TradeSkuOrderBean>("dim_base_category3") {
                    @Override
                    public String getPk(TradeSkuOrderBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public TradeSkuOrderBean join(TradeSkuOrderBean value, JSONObject dimInfo) {
                        value.setCategory3Name(dimInfo.getString("name"));
                        value.setCategory2Id(dimInfo.getString("category2_id"));
                        return value;
                    }
                }, asyncTimeout, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceWithC2DS =
                AsyncDataStream.unorderedWait(reduceWithC3DS,
                new AddDimInfoMultiThreadFunc<TradeSkuOrderBean, TradeSkuOrderBean>("dim_base_category2") {
                    @Override
                    public String getPk(TradeSkuOrderBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public TradeSkuOrderBean join(TradeSkuOrderBean value, JSONObject dimInfo) {
                        value.setCategory2Name(dimInfo.getString("name"));
                        value.setCategory1Id(dimInfo.getString("category1_id"));
                        return value;
                    }
                }, asyncTimeout, TimeUnit.SECONDS);

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceWithC1DS = AsyncDataStream.unorderedWait(reduceWithC2DS,
                new AddDimInfoMultiThreadFunc<TradeSkuOrderBean, TradeSkuOrderBean>("dim_base_category1") {
                    @Override
                    public String getPk(TradeSkuOrderBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public TradeSkuOrderBean join(TradeSkuOrderBean value, JSONObject dimInfo) {
                        value.setCategory1Name(dimInfo.getString("name"));
                        return value;
                    }
                }, asyncTimeout, TimeUnit.SECONDS);
        reduceWithC1DS.print();
//        reduceWithC1DS.map(new MapFunction<TradeSkuOrderBean, String>() {
//                    @Override
//                    public String map(TradeSkuOrderBean value) throws Exception {
//                        SerializeConfig serializeConfig = new SerializeConfig();
//                        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
//                        return JSONObject.toJSONString(value, serializeConfig);
//                    }
//                })
//                .sinkTo(DorisUtil.getDorisSink("gmall_flink.dws_trade_sku_order_window"));

        env.execute();
    }
}
