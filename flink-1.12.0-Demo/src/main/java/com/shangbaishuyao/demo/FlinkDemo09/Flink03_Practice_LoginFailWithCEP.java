package com.shangbaishuyao.demo.FlinkDemo09;

import com.shangbaishuyao.demo.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;
/**
 * Author: shangbaishuyao
 * Date: 13:00 2021/4/23
 * Desc: 登录失败CEP模型
 */
public class Flink03_Practice_LoginFailWithCEP {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取文本数据,转换为JavaBean,提取时间戳生成Watermark
        WatermarkStrategy<LoginEvent> loginEventWatermarkStrategy = WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\flink-1.12.0-Demo\\input\\LoginLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new LoginEvent(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(loginEventWatermarkStrategy);
        //3.按照用户ID分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);
        //4.定义模式序列
//        Pattern<LoginEvent, LoginEvent> loginEventPattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent value) throws Exception {
//                return "fail".equals(value.getEventType());
//            }
//        }).next("next").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent value) throws Exception {
//                return "fail".equals(value.getEventType());
//            }
//        }).within(Time.seconds(5));
        Pattern<LoginEvent, LoginEvent> loginEventPattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        })
                .times(2)      //默认使用的为宽松近邻
                .consecutive() //指定使用严格近邻模式
                .within(Time.seconds(5));

        //5.将模式序列作用于流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, loginEventPattern);
        //6.提取匹配上的事件
        SingleOutputStreamOperator<String> result = patternStream.select(new LoginFailPatternSelectFunc());
        //7.打印结果
        result.print();
        //8.执行任务
        env.execute();
    }
    public static class LoginFailPatternSelectFunc implements PatternSelectFunction<LoginEvent, String> {
        @Override
        public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
            //取出数据
            LoginEvent start = pattern.get("start").get(0);
//            LoginEvent next = pattern.get("next").get(0);
            LoginEvent next = pattern.get("start").get(1);
            //输出结果
            return start.getUserId() + "在 " + start.getEventTime()
                    + " 到 " + next.getEventTime() + " 之间连续登录失败2次！";
        }
    }
}
