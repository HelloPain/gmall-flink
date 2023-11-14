package app.dws;

import bean.TrafficPageViewBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.Common;
import util.DorisUtil;
import util.KafkaUtil;
import util.WindowUtil;

import java.text.SimpleDateFormat;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/10 15:34
 * @Function: 页面浏览数(pv)、浏览总时长(sum)、会话数(last page id is null)、独立访客数(uv 按照mid分组去重,state存时间)
 * @DataLink: mock -> flume -> kafka -> flink(page split) ->
 * *                  kafka(dwd_traffic_page) -> flink -> doris(dws_traffic_vc_ch_ar_is_new_page_view_window)
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        //checkpoint must be set because doris sink is 2pc
        env.enableCheckpointing(10000L);
        env.setStateBackend(new HashMapStateBackend());

        DataStreamSource<String> kafkaDs = env.fromSource(KafkaUtil.getKafkaSource(
                        Common.TOPIC_DWD_TRAFFIC_PAGE, Common.KAFKA_DWS_KEYWORD_SPLIT_GROUP),
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
                //2.key by mid(one user one mid)
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("mid");
                    }
                })
                //3.map to bean, add pv(=1), during_time(sum), uv(=1 if a new user, else =0), sv(=1 if a new session, else =0)
                .map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {

                    ValueState<String> lastDateState;
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("lastDateState", String.class);
                        stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastDateState = getRuntimeContext().getState(stateDescriptor);
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public TrafficPageViewBean map(JSONObject value) throws Exception {
                        JSONObject commonObj = value.getJSONObject("common");
                        JSONObject pageObj = value.getJSONObject("page");
                        Long ts = value.getLong("ts");
                        String curDate = sdf.format(ts);

                        //Session duplication
                        long sv = 0L;
                        //no last page id means a new session
                        if (pageObj.getString("last_page_id") == null) {
                            sv = 1L;
                        }

                        //UV duplication
                        long uv = 0L;
                        if (!curDate.equals(lastDateState.value())) {
                            uv = 1L;
                            lastDateState.update(curDate);
                        }

                        return new TrafficPageViewBean(
                                "",
                                "",
                                commonObj.getString("vc"),
                                commonObj.getString("ch"),
                                commonObj.getString("ar"),
                                commonObj.getString("is_new"),
                                curDate,
                                uv,
                                sv,
                                1L,
                                pageObj.getLong("during_time"),
                                ts
                        );
                    }
                })
                //4.key by window reduce
                .keyBy(new KeySelector<TrafficPageViewBean, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public Tuple5<String, String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                        return Tuple5.of(value.getVc(), value.getCh(), value.getAr(), value.getIsNew(), value.getCurDate());
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(Common.WINDOW_SIZE_SECOND)))
                .reduce(new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple5<String, String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple5<String, String, String, String, String> keyTuple5,
                                      TimeWindow window,
                                      Iterable<TrafficPageViewBean> input,
                                      Collector<TrafficPageViewBean> out) throws Exception {
                        WindowUtil.<TrafficPageViewBean>addWindowInfo(window, input, out);
                    }
                })
                //.print();
                //5.sink to doris
                .map(new MapFunction<TrafficPageViewBean, String>() {
                    @Override
                    public String map(TrafficPageViewBean value) throws Exception {
                        SerializeConfig config = new SerializeConfig();
                        config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
                        return JSON.toJSONString(value, config);
                    }
                })
                .sinkTo(DorisUtil.getDorisSink("gmall_flink.dws_traffic_vc_ch_ar_is_new_page_view_window"));


        env.execute("Dws02_TrafficVcChArIsNewPageViewWindow");
    }
}
