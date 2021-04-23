package com.shangbaishuyao.demo.FlinkDemo08;

import com.shangbaishuyao.demo.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
/**
 * Author: shangbaishuyao
 * Date: 12:38 2021/4/23
 * Desc: 订单支付实时监控
 *
 *  在电商网站中，订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。
 * 	对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。
 * 	另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。
 */
public class Flink06_Practice_OrderPay {
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
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\flink-1.12.0-Demo\\input\\OrderLog.csv")
//        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new OrderEvent(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);

        //3.按照OrderID进行分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);
        //4.使用状态编程+定时器方式实现超时订单的获取
        SingleOutputStreamOperator<String> result = keyedStream.process(new OrderPayProcessFunc(15));
        //5.打印数据
        result.print();
        result.getSideOutput(new OutputTag<String>("Payed TimeOut Or No Create") {}).print("No Create");
        result.getSideOutput(new OutputTag<String>("No Pay") {}).print("No Pay");
        //6.执行任务
        env.execute();
    }
    public static class OrderPayProcessFunc extends KeyedProcessFunction<Long, OrderEvent, String> {
        private Integer interval;
        public OrderPayProcessFunc(Integer interval) {
            this.interval = interval;
        }
        //声明状态
        private ValueState<OrderEvent> createState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("create-state", OrderEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }
        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            //判断当前的数据中的类型
            if ("create".equals(value.getEventType())) {
                //更新状态
                createState.update(value);
                //注册interval分钟以后的定时器
                long ts = (value.getEventTime() + interval * 60) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerTsState.update(ts);
            } else if ("pay".equals(value.getEventType())) {
                //取出状态中的数据
                OrderEvent orderEvent = createState.value();
                //判断创建数据是否为Null
                if (orderEvent == null) {
                    //丢失了创建数据,或者超过15分钟才支付
                    ctx.output(new OutputTag<String>("Payed TimeOut Or No Create") {}, value.getOrderId() + " Payed But No Create！");
                } else {
                    //结合写出
                    out.collect(value.getOrderId() +
                            " Create at " +
                            orderEvent.getEventTime() +
                            " Payed at " + value.getEventTime());
                    //删除定时器
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());

                    //清空状态
                    createState.clear();
                    timerTsState.clear();
                }
            }
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //取出状态中的数据
            OrderEvent orderEvent = createState.value();

            ctx.output(new OutputTag<String>("No Pay") {
                       },
                    orderEvent.getOrderId() + " Create But No Pay!");
            //清空状态
            createState.clear();
            timerTsState.clear();
        }
    }
}
