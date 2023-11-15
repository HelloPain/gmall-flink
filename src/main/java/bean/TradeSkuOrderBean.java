package bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import util.ISetWindowInfo;

import java.math.BigDecimal;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/14 10:41
 * @Function:
 */
@Data
@AllArgsConstructor
@Builder
public class TradeSkuOrderBean implements ISetWindowInfo {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // 二级品类 ID
    String category2Id;
    // 二级品类名称
    String category2Name;
    // 三级品类 ID
    String category3Id;
    // 三级品类名称
    String category3Name;
    // sku_id
    String skuId;
    // sku 名称
    String skuName;
    // spu_id
    String spuId;
    // spu 名称
    String spuName;
    String curDate;
    // 原始金额
    @Builder.Default
    BigDecimal originalAmount = BigDecimal.ZERO;
    // 活动减免金额
    @Builder.Default
    BigDecimal activityAmount = BigDecimal.ZERO;
    // 优惠券减免金额
    @Builder.Default
    BigDecimal couponAmount = BigDecimal.ZERO;
    // 下单金额
    @Builder.Default
    BigDecimal orderAmount = BigDecimal.ZERO;

    //    public static void main(String[] args) {
//        TradeSkuOrderBean bean = TradeSkuOrderBean.builder().activityAmount(BigDecimal.ZERO).build();
//        System.out.println("bean = " + bean);
//    }
}

