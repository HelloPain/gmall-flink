package util;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.producer.ProducerRecord;


import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/3 14:00
 */
public class KafkaUtil {
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Common.KAFKA_SERVERS)
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        //SimpleStringSchema can't handle null message,
                        //so we need to new DeserializationSchema
                        if (message == null) {
                            return null;
                        }
                        return new String(message);
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false; // kafka is unlimited stream
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();
    }

    public static <T> KafkaSink<T> getKafkaSink(KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema){
        return KafkaSink.<T>builder()
                .setBootstrapServers(Common.KAFKA_SERVERS)
                .setRecordSerializer(kafkaRecordSerializationSchema)
                .build();
    }
}
