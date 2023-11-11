package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import util.ISetWindowInfo;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/11 10:26
 * @Function:
 */
@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean implements ISetWindowInfo {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    String curDate;
    // 首页独立访客数
    Long homeUvCt;
    // 商品详情页独立访客数
    Long goodDetailUvCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;

}
