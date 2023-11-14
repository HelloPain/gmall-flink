package app.dws;

import bean.TrafficHomeDetailPageViewBean;

import java.text.SimpleDateFormat;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.Common;
import util.DorisUtil;
import util.KafkaUtil;
import util.WindowUtil;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/11 10:25
 * @Function: 从 Kafka页面日志主题读取数据，统计当日的首页和商品详情页独立访客数。
 */
public class DwsTrafficHomeDetailPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        //checkpoint must be set because doris sink is 2pc
        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());

        env.fromSource(KafkaUtil.getKafkaSource(
                                Common.TOPIC_DWD_TRAFFIC_PAGE, Common.KAFKA_DWS_HOME_DETAIL_UV_GROUP),
                        WatermarkStrategy.noWatermarks(), "kafkaSource")
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
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts")))
                //2.key by mid, one user one mid, prepare for duplication
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("mid");
                    }
                })
                .flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
                    ValueState<String> homePageDt;
                    ValueState<String> detailPageDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> homePageStateDesc = new ValueStateDescriptor<>("homePageDt", String.class);
                        homePageStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        homePageDt = getRuntimeContext().getState(homePageStateDesc);

                        ValueStateDescriptor<String> detailPageStateDesc = new ValueStateDescriptor<>("detailPageDt", String.class);
                        detailPageStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        detailPageDt = getRuntimeContext().getState(detailPageStateDesc);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        String curDt = sdf.format(value.getLong("ts"));
                        JSONObject pageObj = value.getJSONObject("page");

                        long homeUv = 0L;
                        long detailUv = 0L;
                        if (pageObj.getString("page_id").equals("home")) {
                            String lastDt = homePageDt.value();
                            if (lastDt == null || !lastDt.equals(curDt)) {
                                homeUv = 1L;
                                homePageDt.update(curDt);
                            }
                        } else if (pageObj.getString("page_id").equals("good_detail")) {
                            String lastDt = detailPageDt.value();
                            if (lastDt == null || !lastDt.equals(curDt)) {
                                detailUv = 1L;
                                detailPageDt.update(curDt);
                            }
                        }
                        if (homeUv != 0 || detailUv != 0) {
                            out.collect(new TrafficHomeDetailPageViewBean(
                                    "",
                                    "",
                                    curDt,
                                    homeUv,
                                    detailUv,
                                    value.getLong("ts")
                            ));
                        }
                    }
                })
                .keyBy(TrafficHomeDetailPageViewBean::getCurDate)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(Common.WINDOW_SIZE_SECONDS)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt((value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt()));
                        return value1;
                    }
                }, new WindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> input, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        WindowUtil.addWindowInfo(window, input, out);
                    }
                })
                .map(new MapFunction<TrafficHomeDetailPageViewBean, String>() {
                    @Override
                    public String map(TrafficHomeDetailPageViewBean value) throws Exception {
                        SerializeConfig serializeConfig = new SerializeConfig();
                        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
                        return JSONObject.toJSONString(value, serializeConfig);
                    }
                })
                .sinkTo(DorisUtil.getDorisSink("gmall_flink.dws_traffic_home_detail_page_view_window"));
        //.print();

        env.execute("dws_traffic_home_detail_page_view_window");
    }
}
