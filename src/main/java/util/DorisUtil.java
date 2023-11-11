package util;

import lombok.Data;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;

import java.util.Properties;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/10 16:28
 * @Function:
 */
@Data
public class DorisUtil {

    static Integer maxRetries = 2;
    static Integer checkInterval = 2;

    public static DorisSink<String> getDorisSink(String tableName){
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");

        DorisOptions.Builder dorisOptions = DorisOptions.builder()
                .setFenodes(Common.DORIS_FE_NODES)
                .setTableIdentifier(tableName)
                .setUsername(Common.DORIS_USERNAME)
                .setPassword(Common.DORIS_PASSWORD);

        return DorisSink.<String>builder()
                .setDorisOptions(dorisOptions.build())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(
                        DorisExecutionOptions.builder()
                                .setStreamLoadProp(properties)
                                .setDeletable(false)
                                .disable2PC()
                                .setLabelPrefix("doris-")
                                .setCheckInterval(checkInterval)
                                .setMaxRetries(maxRetries)
                                .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
