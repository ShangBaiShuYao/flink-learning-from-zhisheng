package com.shangbaishuyao.gmall.realtime.app.Function;

import com.shangbaishuyao.gmall.realtime.utils.IKUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Author: shangbaishuyao
 * Date: 2021/2/26
 * Desc:  自定义UDTF函数实现分词操作
 *
 * Flink                Hive      解释
 * scalar function      UDF       一对一
 * Aggregation function UDAF      多对一
 * Table function       UDTF      一对多
 *
 */                                  //直接返回单词就好,单词长度不要了
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String value) {
        //使用工具类对字符串进行分词
        List<String> keywordList = IKUtil.analyze(value);
        for (String keyword : keywordList) {
            collect(Row.of(keyword)); //这个的底层其实就是下面这种写法
            /*Row row = new Row(1);
            row.setField(0,keyword);
            collect(row);*/
        }
    }
}
