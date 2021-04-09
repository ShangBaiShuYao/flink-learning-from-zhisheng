package com.shangbaishuyao.gmall.realtime.app.func;

import com.shangbaishuyao.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Author: shangbaishuyao
 * Date: 2021/2/26
 * Desc:  自定义UDTF函数实现分词操作
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String value) {
        //使用工具类对字符串进行分词
        List<String> keywordList = KeywordUtil.analyze(value);
        for (String keyword : keywordList) {
            //collect(Row.of(keyword));
            Row row = new Row(1);
            row.setField(0,keyword);
            collect(row);
        }
    }
}
