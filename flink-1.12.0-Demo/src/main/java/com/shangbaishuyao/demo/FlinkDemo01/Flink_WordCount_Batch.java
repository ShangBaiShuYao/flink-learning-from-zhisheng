package com.shangbaishuyao.demo.FlinkDemo01;

import com.shangbaishuyao.demo.Function.MyFlatMapFunc;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Author: shangbaishuyao
 * Date: 0:24 2021/4/22
 * Desc: Flink 批处理案例.
 *
 * TODO 在Java中,二元组Tuple就是2,即Tuple2;三元组就是3,即Tuple3
 */
public class Flink_WordCount_Batch {
    public static void main(String[] args)throws Exception{
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取文本文件
        DataSource<String> input = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\flink-1.12.0-Demo\\input\\word.txt");
        /**
         * shangbaishuyao
         * hive
         * hello
         */
        //压平
        FlatMapOperator<String, String> word = input.flatMap(new MyFlatMapFunc());
        //将单词转换为元组 (hello,1) (shangbaishuyao,1)
        MapOperator<String, Tuple2<String, Integer>> wordToOne = word.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //TODO 1或者2都可以
                Tuple2<String, Integer> tuple1 = new Tuple2<String, Integer>(value, 1);
                Tuple2<String, Integer> tuple2 = Tuple2.of(value, 1);
                return tuple2;
            }
        });
        //在spark里面,到这一步直接reduceByKey结束了, 但是Flink是流处理,没有reduceByKey这种操作.Flink得先分组
        //分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);//按照元组的第一位分组
        //聚合 Flink的分组和聚合是分开的,先分组再聚合
        AggregateOperator<Tuple2<String, Integer>> sum = groupBy.sum(1); //按照元组1号聚合
        //7.打印结果
        sum.print();
    }
}
