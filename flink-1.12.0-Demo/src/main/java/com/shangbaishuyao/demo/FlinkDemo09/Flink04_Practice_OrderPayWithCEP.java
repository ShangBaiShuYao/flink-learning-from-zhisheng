package com.shangbaishuyao.demo.FlinkDemo09;

import com.shangbaishuyao.demo.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
/**
 * Author: shangbaishuyao
 * Date: 12:46 2021/4/23
 * Desc: 支付失败CEP模型
 */
public class Flink04_Practice_OrderPayWithCEP {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取文本数据创建流,转换为JavaBean,提取时间戳生成Watermark
        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new OrderEvent(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);
        //3.按照OrderID进行分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);
        //4.定义模式序列
        Pattern<OrderEvent, OrderEvent> orderEventPattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));
        //5.将模式序列作用于流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, orderEventPattern);
        //6.提取正常匹配上的以及超时事件
        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("No Pay") {},
                new OrderPayTimeOutFunc(),
                new OrderPaySelectFunc());
        //7.打印数据
        result.print();
        result.getSideOutput(new OutputTag<String>("No Pay") {
        }).print("Time Out");
        //8.执行任务
        env.execute();
    }
    public static class OrderPayTimeOutFunc implements PatternTimeoutFunction<OrderEvent, String> {
        @Override
        public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            //提取事件
            OrderEvent createEvent = pattern.get("start").get(0);
            //输出数据  侧输出流
            return createEvent.getOrderId() + "在 " + createEvent.getEventTime() +
                    " 创建订单，并在 " + timeoutTimestamp / 1000L + " 超时";
        }
    }
    public static class OrderPaySelectFunc implements PatternSelectFunction<OrderEvent, String> {
        @Override
        public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
            //提取事件
            OrderEvent createEvent = pattern.get("start").get(0);
            OrderEvent payEvent = pattern.get("follow").get(0);
            //输出结果  主流
            return createEvent.getOrderId() + "在 " + createEvent.getEventTime() +
                    " 创建订单，并在 " + payEvent.getEventTime() + " 完成支付！！！";
        }
    }
}
