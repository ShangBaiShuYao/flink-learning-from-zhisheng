package com.shangbaishuyao.demo.practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
/*
 * Author: shangbaishuyao
 * Date: 13:51 2021/4/23
 * Desc:
 */
public class Flink04_WordCount_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> readTextFile = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.12.0-Demo/input/word.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = readTextFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOne.keyBy(data -> data.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + 1);
            }
        });
        result.print();
        env.execute();
    }
}