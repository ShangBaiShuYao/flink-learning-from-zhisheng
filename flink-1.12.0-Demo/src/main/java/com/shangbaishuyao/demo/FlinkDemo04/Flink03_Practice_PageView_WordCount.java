package com.shangbaishuyao.demo.FlinkDemo04;

import com.shangbaishuyao.demo.bean.UserBehavior;
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
 * Desc: 网站总浏览量（PV）的统计
 * pv实现思路1: WordCount
 *
 * PV:
 * 衡量网站流量一个最简单的指标，就是网站的页面浏览量（Page View，PV）。
 * 用户每次打开一个页面便记录1次PV，多次打开同一页面则浏览量累计。
 * 	一般来说，PV与来访者的数量成正比，但是PV并不直接决定页面的真实来访者数量，
 * 	如同一个来访者通过不断的刷新页面，也可以制造出非常高的PV。
 * 	接下来我们就用咱们之前学习的Flink算子来实现PV的统计。
 */
public class Flink03_Practice_PageView_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile(
                "/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.12.0-Demo/input/UserBehavior.csv");
        //3.转换为JavaBean并过滤出PV的数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = readTextFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //a.按照","分割
                String[] split = value.split(",");
                //b.封装JavaBean对象
                UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));
                //c.选择需要输出的数据
                if ("pv".equals(userBehavior.getBehavior())) {
                    out.collect(new Tuple2<>("PV", 1));
                }
            }
        });
        //4.指定Key分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = pv.keyBy(data -> data.f0);
        //5.计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        //6.打印输出
        result.print();
        //7.执行任务
        env.execute();
    }
}
