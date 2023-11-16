package app.dws;

import bean.UserRegisterBean;
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
import util.*;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/11 11:31
 * @Function: 从 DWD层用户注册表中读取数据，统计各窗口注册用户数，写入 Doris。
 */
public class DwsUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        //checkpoint must be set because doris sink is 2pc
        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());

        DataStreamSource<String> kafkaDs = env.fromSource(KafkaUtil.getKafkaSource(
                        Common.TOPIC_DWD_USER_REGISTER, Common.KAFKA_DWD_USER_REGISTER_GROUP),
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
                        .withTimestampAssigner((element, recordTimestamp) -> element.getLong("create_time")))
                //2.key by mid, one user one mid, prepare for duplication
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("id");
                    }
                })
                //3.cal uv and back uv, output UserRegisterBean
                .flatMap(new RichFlatMapFunction<JSONObject, UserRegisterBean>() {
                    ValueState<String> curDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> curDateStateDescriptor = new ValueStateDescriptor<>("curDateStateDescriptor", String.class);
                        curDateStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());
                        curDateState = getRuntimeContext().getState(curDateStateDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<UserRegisterBean> out) throws Exception {
                        Long ts = value.getLong("create_time");
                        String curDt = DateFormatUtil.toDate(ts);

                        //duplication for uv
                        if (curDateState.value() == null || !curDateState.value().equals(curDt)) {
                            curDateState.update(curDt);
                            out.collect(new UserRegisterBean("", "", curDt, 1L, ts));
                        }
                    }
                })
                //4.key by curDate, window by 10s, reduce
                .keyBy((KeySelector<UserRegisterBean, String>) UserRegisterBean::getCurDate)
                .window(TumblingEventTimeWindows.of(Time.seconds(Common.WINDOW_SIZE_SECONDS)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new WindowFunction<UserRegisterBean, UserRegisterBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<UserRegisterBean> input, Collector<UserRegisterBean> out) throws Exception {
                        WindowUtil.<UserRegisterBean>addWindowInfo(window, input, out);
                    }
                })
//                .print();
                .map(new MapFunction<UserRegisterBean, String>() {
                    @Override
                    public String map(UserRegisterBean value) throws Exception {
                        SerializeConfig serializeConfig = new SerializeConfig();
                        serializeConfig.setPropertyNamingStrategy(com.alibaba.fastjson.PropertyNamingStrategy.SnakeCase);
                        return JSONObject.toJSONString(value, serializeConfig);
                    }
                })
                //5.sink to doris
                .sinkTo(DorisUtil.getDorisSink("gmall_flink.dws_user_user_register_window"));

        env.execute();
    }
}
