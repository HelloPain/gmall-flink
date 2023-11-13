package bean;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/13 13:56
 * @Function:
 */

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import util.ISetWindowInfo;

/**
 * Desc: 交易域下单实体类
 */
@Data
@AllArgsConstructor
@Builder
public class TradeOrderBean implements ISetWindowInfo {
    // 窗口起始时间
    String stt;
    // 窗口关闭时间
    String edt;
    String curDate;
    // 下单独立用户数
    Long orderUniqueUserCount;
    // 下单新用户数
    Long orderNewUserCount;
}

