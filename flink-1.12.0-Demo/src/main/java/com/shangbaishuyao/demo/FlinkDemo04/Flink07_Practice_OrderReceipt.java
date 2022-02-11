package com.shangbaishuyao.demo.FlinkDemo04;

import com.shangbaishuyao.demo.bean.OrderEvent;
import com.shangbaishuyao.demo.bean.TxEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
/**
 * Author: shangbaishuyao
 * Date: 18:06 2021/4/22
 * Desc: 订单支付实时监控
 *
 * 在电商网站中，订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。
 * 对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，
 * 网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。
 * 另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。
 *
 * 需求: 来自两条流的订单交易匹配
 * 	对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账了。
 * 	而往往这会来自不同的日志信息，所以我们要同时读入两条流的数据来做合并处理。
 */
public class Flink07_Practice_OrderReceipt {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取2个文本数据创建流
        DataStreamSource<String> orderStreamDS = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.12.0-Demo/input/OrderLog.csv");
        DataStreamSource<String> receiptStreamDS = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.12.0-Demo/input/ReceiptLog.csv");
        //3.转换为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderEventDS = orderStreamDS.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                String[] split = value.split(",");
                OrderEvent orderEvent = new OrderEvent(Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3]));
                if ("pay".equals(orderEvent.getEventType())) {
                    out.collect(orderEvent);
                }
            }
        });
        SingleOutputStreamOperator<TxEvent> txDS = receiptStreamDS.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        });
        //4.按照TXID进行分组
        KeyedStream<OrderEvent, String> orderEventStringKeyedStream = orderEventDS.keyBy(OrderEvent::getTxId);
        KeyedStream<TxEvent, String> txEventKeyedStream = txDS.keyBy(TxEvent::getTxId);
        //5.连接两个流
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = orderEventStringKeyedStream.connect(txEventKeyedStream);
        //6.处理两条流的数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = connectedStreams.process(new MyCoKeyedProcessFunc());
        //7.打印结果
        result.print();
        //8.执行任务
        env.execute();
    }

    //双流合并
    public static class MyCoKeyedProcessFunc extends KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>> {
        private HashMap<String, OrderEvent> orderEventHashMap = new HashMap<>();
        private HashMap<String, TxEvent> txEventHashMap = new HashMap<>();
        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            if (txEventHashMap.containsKey(value.getTxId())) {
                TxEvent txEvent = txEventHashMap.get(value.getTxId());
                out.collect(new Tuple2<>(value, txEvent));
            } else {
                orderEventHashMap.put(value.getTxId(), value);
            }
        }
        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            if (orderEventHashMap.containsKey(value.getTxId())) {
                OrderEvent orderEvent = orderEventHashMap.get(value.getTxId());
                out.collect(new Tuple2<>(orderEvent, value));
            } else {
                txEventHashMap.put(value.getTxId(), value);
            }
        }
    }
}
