package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import util.ISetWindowInfo;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/11 13:50
 * @Function:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean implements ISetWindowInfo {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    String curDate;
    // 回流用户数
    Long backCt;
    // 独立用户数
    Long uuCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
