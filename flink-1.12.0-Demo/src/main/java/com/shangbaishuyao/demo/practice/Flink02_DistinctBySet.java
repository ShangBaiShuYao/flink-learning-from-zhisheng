package com.shangbaishuyao.demo.practice;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;
/*
 * Author: shangbaishuyao
 * Date: 13:49 2021/4/23
 * Desc:
 */
public class Flink02_DistinctBySet {
    public static void main(String[] args) throws Exception {
        HashSet<String> hashSet = new HashSet<>();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<String> wordDS = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        wordDS.keyBy(x -> x)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (hashSet.contains(value)) {
                            return false;
                        } else {
                            hashSet.add(value);
                            return true;
                        }
                    }
                }).print();
        env.execute();
    }
}
