package com.shangbaishuyao.demo.FlinkDemo04;

import com.shangbaishuyao.demo.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
/**
 * Author: shangbaishuyao
 * Date: 17:51 2021/4/22
 * Desc:网站总浏览量（PV）的统计
 * pv实现思路2: process
 *
 * 有数据倾斜的问题,如何解决呢? 加随机数.
 */
public class Flink04_Practice_PageView_Process {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile(
                "/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.12.0-Demo/input/UserBehavior.csv");
        //3.转换为JavaBean并过滤出PV的数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = readTextFile.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
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
                    out.collect(userBehavior);
                }
            }
        });
        //4.指定Key分组
        KeyedStream<UserBehavior, String> keyedStream = userBehaviorDS.keyBy(data -> "PV");
        //5.计算总和
        SingleOutputStreamOperator<Integer> result = keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {
            Integer count = 0;
            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                count++;
                out.collect(count);
            }
        });
        //6.打印输出
        result.print();
        //7.执行任务
        env.execute();
    }
}
