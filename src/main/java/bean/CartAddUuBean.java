package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import util.ISetWindowInfo;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/13 20:57
 * @Function:
 */
@Data
@AllArgsConstructor
public class CartAddUuBean implements ISetWindowInfo {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}

