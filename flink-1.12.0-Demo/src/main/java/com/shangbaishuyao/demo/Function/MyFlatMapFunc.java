package com.shangbaishuyao.demo.Function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Author: shangbaishuyao
 * Date: 0:32 2021/4/22
 * Desc: 自定义实现压平操作的类
 */
public class MyFlatMapFunc implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        //按照空格切割
        String[] words = value.split(" ");
        //遍历words,写出一个个的单词
        for (String word : words) {
            out.collect(word);
        }
    }
}
