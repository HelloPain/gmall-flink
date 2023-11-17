package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import util.ISetWindowInfo;

import java.math.BigDecimal;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/17 9:35
 * @Function:
 */
@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderBean implements ISetWindowInfo {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 省份 ID
    String provinceId;
    // 省份名称
    @Builder.Default
    String provinceName = "";
    // 订单 ID
    @JSONField(serialize = false)
    String orderId;
    String curDate;
    // 累计下单次数
    Long orderCount;
    // 累计下单金额
    BigDecimal orderAmount;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;

}
