package app.dws;

import bean.UserLoginBean;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

import java.text.SimpleDateFormat;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/11 11:31
 * @Function: 从 Kafka 页面日志主题读取数据，统计七日回流（回归）用户和当日独立用户数。
 */
public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        //checkpoint must be set because doris sink is 2pc
        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());
        long loginInterval = 24 * 60 * 60 * 7 * 1000L;


        DataStreamSource<String> kafkaDs = env.fromSource(KafkaUtil.getKafkaSource(
                        Common.TOPIC_DWD_TRAFFIC_PAGE, Common.KAFKA_DWS_CALLBACK_UV_GROUP),
                WatermarkStrategy.noWatermarks(), "kafkaSource");

        kafkaDs
                //1.turn to json object
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
                        .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts")))
                //2.key by mid, one user one mid, prepare for duplication
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("mid");
                    }
                })
                //3.cal uv and back uv, output UserLoginBean
                .flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

                    ValueState<Long> lastVisitDateState;
                    ValueState<String> curDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Long> lastVisitDateStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", Long.class);
                        lastVisitDateStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDescriptor);

                        ValueStateDescriptor<String> curDateStateDescriptor = new ValueStateDescriptor<>("curDateStateDescriptor", String.class);
                        curDateStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                        curDateState = getRuntimeContext().getState(curDateStateDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Long ts = value.getLong("ts");
                        String curDt = sdf.format(ts);

                        //duplication for uv
                        long todayUv = 0L;
                        if (curDateState.value() == null || !curDateState.value().equals(curDt)) {
                            todayUv = 1L;
                            curDateState.update(curDt);
                        }

                        //only user who visit today can be a back user
                        if (todayUv != 0) {
                            long backUv = 0L;
                            if (lastVisitDateState.value() != null && ts - lastVisitDateState.value() >= loginInterval) {
                                backUv = 1L;
                            }
                            lastVisitDateState.update(value.getLong("ts"));

                            out.collect(new UserLoginBean(
                                    "",
                                    "",
                                    curDt,
                                    backUv,
                                    todayUv,
                                    ts
                            ));
                        }
                    }
                })
                //4.key by curDate, window by 10s, reduce
                .keyBy((KeySelector<UserLoginBean, String>) UserLoginBean::getCurDate)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                }, new WindowFunction<UserLoginBean, UserLoginBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<UserLoginBean> input, Collector<UserLoginBean> out) throws Exception {
                        WindowUtil.<UserLoginBean>addWindowInfo(window, input, out);
                    }
                })
                .map(new MapFunction<UserLoginBean, String>() {
                    @Override
                    public String map(UserLoginBean value) throws Exception {
                        SerializeConfig serializeConfig = new SerializeConfig();
                        serializeConfig.setPropertyNamingStrategy(com.alibaba.fastjson.PropertyNamingStrategy.SnakeCase);
                        return JSONObject.toJSONString(value, serializeConfig);
                    }
                })
                //5.sink to doris
                .sinkTo(DorisUtil.getDorisSink("gmall_flink.dws_user_user_login_window"));

        env.execute();
    }
}
