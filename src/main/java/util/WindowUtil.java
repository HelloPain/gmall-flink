package util;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/11 8:30
 * @Function:
 */
public class WindowUtil {

    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//thread safe?

    public static <T extends ISetWindowInfo> void addWindowInfo(TimeWindow window,
                                                                Iterable<T> input,
                                                                Collector<T> out) {
        for (T t : input) {
            t.setStt(sdf.format(window.getStart()));
            t.setEdt(sdf.format(window.getEnd()));
            out.collect(t);
        }
    }
}
