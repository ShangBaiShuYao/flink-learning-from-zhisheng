package com.shangbaishuyao.demo.Function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Author: shangbaishuyao
 * Date: 12:56 2021/4/22
 * Desc: 自定义实现压平和转换元组操作的类
 */
public class MyFlatMapFunc2 implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        //按照空格切分数据
        String[] words = value.split(" ");
        //遍历写出
        for (String word : words) {
            //将单词转换为元组
            out.collect(new Tuple2<String, Integer>(word, 1));
        }
    }
}

