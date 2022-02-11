package com.shangbaishuyao.demo.FlinkDemo04;

import com.shangbaishuyao.demo.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
/**
 * UV和PV最大的不同是. UV需要去重.
 * Author: shangbaishuyao
 * Date: 17:59 2021/4/22
 * Desc: 网站独立访客数（UV）的统计
 *
 * UV:
 * 上一个案例中，我们统计的是所有用户对页面的所有浏览行为，也就是说，同一用户的浏览行为会被重复统计。
 * 而在实际应用中，我们往往还会关注，到底有多少不同的用户访问了网站，
 * 所以另外一个统计流量的重要指标是网站的独立访客数（Unique Visitor，UV）
 */
public class Flink05_Practice_UserVisitor {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.12.0-Demo/input/UserBehavior.csv");
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
        //3.指定Key分组
        KeyedStream<UserBehavior, String> keyedStream = userBehaviorDS.keyBy(data -> "UV");
        //4.使用Process方式计算总和(注意UserID的去重)
        SingleOutputStreamOperator<Integer> result = keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {
            private HashSet<Long> uids = new HashSet<>();
            private Integer count = 0;
            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                if (!uids.contains(value.getUserId())) {
                    uids.add(value.getUserId());
                    count++;
                    out.collect(count);
                }
            }
        });
        //5.打印输出
        result.print();
        //6.执行任务
        env.execute();
    }
}
