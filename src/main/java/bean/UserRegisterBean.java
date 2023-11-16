package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import util.ISetWindowInfo;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/16 11:08
 * @Function:
 */
@Data
@AllArgsConstructor
public class UserRegisterBean implements ISetWindowInfo {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    String curDate;
    // 注册用户数
    Long registerCt;
    // 时间戳
    Long ts;
}
