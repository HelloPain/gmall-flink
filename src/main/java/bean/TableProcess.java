package bean;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/3 14:53
 */

import lombok.Data;

/**
 * Desc: 配置表对应实体类
 */
@Data
public class TableProcess {
    // 来源表
    String sourceTable;
    // 来源操作类型
    String sourceType;
    // 输出表
    String sinkTable;
    // 输出类型 dwd | dim
    String sinkType;
    // 输出字段
    String sinkFamily;
    // sink到 hbase 的时候的具体字段
    String sinkColumns;
    // sink到 hbase 的时候的主键字段
    String sinkRowKey;
    // sink 到hbase的时候的分区键
    String sinkExtend;
    //配置表的操作类型
    String op;
}
