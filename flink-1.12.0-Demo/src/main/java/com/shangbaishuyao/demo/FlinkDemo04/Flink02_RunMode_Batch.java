package com.shangbaishuyao.demo.FlinkDemo04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
/**
 * Author: shangbaishuyao
 * Date: 17:46 2021/4/22
 * Desc: 执行模式(Execution Mode) Flink V1.12.0新出的模式.
 * 流出里的环境中可以设置为批处理,但是目前有bug
 */
public class Flink02_RunMode_Batch {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境(默认使用的为流的模式)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 指定为批处理模式
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //2.读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\flink-1.12.0-Demo\\input\\word.txt");
        //3.压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = readTextFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });
        //4.按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOne.keyBy(data -> data.f0);
        //5.计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        //6.打印结果
        result.print();
        //7.执行任务
        env.execute();
    }
}
