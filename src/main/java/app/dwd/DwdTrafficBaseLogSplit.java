package app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.Common;
import util.KafkaUtil;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/6 10:19
 */
public class DwdTrafficBaseLogSplit {
    public static void main(String[] args) throws Exception {
        //分成5个流（启动，曝光，动作，错误，页面），页面到主流
        //is_new这个字段只有在第一天使用的时候是1（登录几次都是1），其他时候是0
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Common.PARALLELISM);

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(Common.TOPIC_ODS_LOG, Common.KAFKA_DWD_LOG_GROUP);

        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action") {
        };
        OutputTag<JSONObject> errorTag = new OutputTag<JSONObject>("error") {
        };

        SingleOutputStreamOperator<JSONObject> pageDs =
                env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                        .flatMap(new FlatMapFunction<String, JSONObject>() {
                            @Override
                            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                                try {
                                    JSONObject jsonObject = JSON.parseObject(value);
                                    out.collect(jsonObject);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        })
                        .keyBy(new KeySelector<JSONObject, String>() { //not necessary?
                            @Override
                            public String getKey(JSONObject value) throws Exception {
                                System.out.println("mid = " + value.getJSONObject("common").getString("mid"));
                                return value.getJSONObject("common").getString("mid");
                            }
                        })
                        .map(new RichMapFunction<JSONObject, JSONObject>() {
                            ValueState<Long> firstDate; //天为单位

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                firstDate = getRuntimeContext().getState(new ValueStateDescriptor<Long>("firstDate", Long.class));
                            }

                            @Override
                            public JSONObject map(JSONObject value) throws Exception {
                                /*新老访客状态标记修复*/
                                //is_new这个字段只有在第一天使用的预期时候是1（登录几次都是1），其他时候是0
                                String isNew = value.getJSONObject("common").getString("is_new");
                                Long curDate = Long.parseLong(value.getString("ts")) / (1000 * 60 * 60 * 24);
                                if ("1".equals(isNew)) {
                                    if (firstDate.value() == null) {//true new
                                        firstDate.update(curDate);
                                    } else if (!curDate.equals(firstDate.value())) {//fake new
                                        value.getJSONObject("common").put("is_new", "0");
                                    }
                                } else {//现在是0，但是未来可能出现1
                                    if (firstDate.value() == null) {//一开始就是0，认为首次登录日期是0，这样后面的数据都会被校正为0
                                        firstDate.update(0L);
                                    }
                                }
                                return value;
                            }
                        })
                        .process(new ProcessFunction<JSONObject, JSONObject>() {
                                     @Override
                                     public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                                         JSONObject start = value.getJSONObject("start");
                                         JSONArray actions = value.getJSONArray("actions");
                                         JSONArray displays = value.getJSONArray("displays");
                                         JSONObject error = value.getJSONObject("err");
                                         JSONObject common = value.getJSONObject("common");
                                         Long ts = value.getLong("ts");
                                         String pageId = value.getJSONObject("page").getString("page_id");


                                         if (error != null) {
                                             ctx.output(errorTag, value);
                                             value.remove("err");
                                         }

                                         if (start != null) {
                                             ctx.output(startTag, value);
                                             return;
                                         } else if (actions != null) {
                                             for (int i = 0; i < actions.size(); i++) {
                                                 JSONObject action = actions.getJSONObject(i);
                                                 action.put("common", common);
                                                 action.put("page_id", pageId);
                                                 ctx.output(actionTag, action);
                                             }
                                             value.remove("actions");
                                         } else if (displays != null) {
                                             for (int i = 0; i < displays.size(); i++) {
                                                 JSONObject display = displays.getJSONObject(i);
                                                 display.put("common", common);
                                                 display.put("page_id", pageId);
                                                 display.put("ts", ts);
                                                 ctx.output(displayTag, display);
                                             }
                                             value.remove("display");
                                         }
                                         out.collect(value);
                                     }
                                 }
                        );


//        pageDs.print("pageDs>>");
//        pageDs.getSideOutput(startTag).print("startTag>>");
//        pageDs.getSideOutput(displayTag).print("displayTag>>");
//        pageDs.getSideOutput(actionTag).print("actionTag>>");
//        pageDs.getSideOutput(errorTag).print("errorTag>>");

        pageDs.sinkTo(KafkaUtil.getKafkaSink(Common.TOPIC_DWD_TRAFFIC_PAGE));
        pageDs.getSideOutput(startTag).sinkTo(KafkaUtil.getKafkaSink(Common.TOPIC_DWD_TRAFFIC_START));
        pageDs.getSideOutput(displayTag).sinkTo(KafkaUtil.getKafkaSink(Common.TOPIC_DWD_TRAFFIC_DISPLAY));
        pageDs.getSideOutput(actionTag).sinkTo(KafkaUtil.getKafkaSink(Common.TOPIC_DWD_TRAFFIC_ACTION));
        pageDs.getSideOutput(errorTag).sinkTo(KafkaUtil.getKafkaSink(Common.TOPIC_DWD_TRAFFIC_ERR));

        env.execute();
    }
}
