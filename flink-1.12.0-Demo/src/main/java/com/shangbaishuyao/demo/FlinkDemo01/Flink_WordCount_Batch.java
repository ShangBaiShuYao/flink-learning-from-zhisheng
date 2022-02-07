package com.shangbaishuyao.demo.FlinkDemo01;

import com.shangbaishuyao.demo.Function.MyFlatMapFunc3;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Author: shangbaishuyao
 * Date: 0:24 2021/4/22
 * Desc: Flink 批处理案例.
 *
 * TODO 在Java中,二元组Tuple就是2,即Tuple2;三元组就是3,即Tuple3
 */
public class Flink_WordCount_Batch {
    public static void main(String[] args)throws Exception{
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataSource<String> lineDataStream = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.14.3-Demo/input/word.txt");
        //压平("1",1) ("2",1) ("3",1) ("4",1)
        FlatMapOperator<String, Tuple2<String, Integer>> tuple2FlatMapOperator = lineDataStream.flatMap(new MyFlatMapFunc3());
        //也可以直接使用map将单词转换为元组
     /*   MapOperator<String, Tuple2<String, Integer>> tuple2MapOperator = lineDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] wordsArray = value.split(" ");
                for (String word : wordsArray) {
                    out.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });*/
        //分组 元组是从0开始计数
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = tuple2FlatMapOperator.groupBy(0);
        //聚合
        AggregateOperator<Tuple2<String, Integer>> result = tuple2UnsortedGrouping.sum(1);
        //打印
        result.print();
    }
}
