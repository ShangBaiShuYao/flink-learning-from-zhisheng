package com.shangbaishuyao.demo.Function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author shangbaishuyao
 * @create 2022-02-07 下午12:26
 */

public class MyFlatMapFunc3 implements FlatMapFunction<String,Tuple2<String,Integer>> {
    @Override
    public void flatMap(String input, Collector<Tuple2<String,Integer>> out) throws Exception {
        //一行一行按照空格切分,一个空格就是一个单词 ["1","2"] ["3","4]
        String[] words = input.split(" ");
        //给数据一个计数标记
        for (String word: words){
            //将单词转换为二元组 ("1",1) ("2",1) ("3",1) ("4",1)
            out.collect(new Tuple2<String,Integer>(word,1));
        }
    }
}
