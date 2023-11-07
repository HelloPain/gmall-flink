package app.dim.func;

import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import util.Common;
import util.HBaseUtil;

import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/3 15:14
 */
public class DimCreateTableMapFunction extends RichMapFunction<String, TableProcess> {

    Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getConnection();
    }

    @Override
    public TableProcess map(String value) throws IOException {
        JSONObject obj = JSON.parseObject(value);
        String op = obj.getString("op");
        TableProcess res;
        if ("d".equals(op)) {
            res = JSON.parseObject(obj.getString("before"), TableProcess.class);
        } else {
            res = JSON.parseObject(obj.getString("after"), TableProcess.class);
        }
        res.setOp(op);

        if("dim".equals(res.getSinkType())){
            if("d".equals(op)){//drop table for d
                HBaseUtil.dropTable(hbaseConn, Common.HBASE_NAMESPACE, res.getSinkTable());
            } else if ("u".equals(op)) {//drop and create table for u
                HBaseUtil.dropTable(hbaseConn, Common.HBASE_NAMESPACE, res.getSinkTable());
                byte[][] splitKeys = HBaseUtil.getSplitKeys(res.getSinkExtend());
                HBaseUtil.createTable(hbaseConn, Common.HBASE_NAMESPACE, res.getSinkTable(), splitKeys, res.getSinkFamily().split(","));
            } else{// create table for c
                byte[][] splitKeys = HBaseUtil.getSplitKeys(res.getSinkExtend());
                HBaseUtil.createTable(hbaseConn, Common.HBASE_NAMESPACE, res.getSinkTable(), splitKeys, res.getSinkFamily().split(","));
            }
        }

        return res;
    }
}
