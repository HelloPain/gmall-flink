package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import util.ISetWindowInfo;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/16 14:56
 * @Function:
 */
@Data
@AllArgsConstructor
public class TradePaymentBean implements ISetWindowInfo {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 支付成功独立用户数
    String curDate;
    Long paymentSucUniqueUserCt;
    // 支付成功新用户数
    Long paymentSucNewUserCt;
}
