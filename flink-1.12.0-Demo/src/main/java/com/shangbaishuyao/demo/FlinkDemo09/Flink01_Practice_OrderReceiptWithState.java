package com.shangbaishuyao.demo.FlinkDemo09;

import com.shangbaishuyao.demo.bean.OrderEvent;
import com.shangbaishuyao.demo.bean.TxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
/**
 * Author: shangbaishuyao
 * Date: 12:45 2021/4/23
 * Desc: 订单和订单明细和状态
 */
public class Flink01_Practice_OrderReceiptWithState {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取2个文本数据创建流
        DataStreamSource<String> orderStreamDS = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\flink-1.12.0-Demo\\input\\OrderLog.csv");
        DataStreamSource<String> receiptStreamDS = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\flink-1.12.0-Demo\\input\\ReceiptLog.csv");
        //3.转换为JavaBean并提取数据中的时间戳生成Watermark
        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });
        WatermarkStrategy<TxEvent> txEventWatermarkStrategy = WatermarkStrategy.<TxEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<TxEvent>() {
                    @Override
                    public long extractTimestamp(TxEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });
        //转换为样例类对象,设置watermark
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
        }).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);
        SingleOutputStreamOperator<TxEvent> txDS = receiptStreamDS.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        }).assignTimestampsAndWatermarks(txEventWatermarkStrategy);

        //  connect: 在某些情况下，我们需要将两个不同来源的数据流进行连接，实现数据匹配，比如订单支付和第三方交易信息，这两个信息的数据就来自于不同数据源，连接后，将订单支付和第三方交易信息进行对账，此时，才能算真正的支付完成。
        //	Flink中的connect算子可以连接两个保持他们类型的数据流，两个数据流被connect之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
        //4.连接支付流和到账流
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = orderEventDS
                .connect(txDS)
                .keyBy("txId", "txId")
                .process(new OrderReceiptKeyedProcessFunc());

        //5.打印数据
        result.print();
        result.getSideOutput(new OutputTag<String>("Payed No Receipt") {
        }).print("No Receipt");
        result.getSideOutput(new OutputTag<String>("Receipt No Payed") {
        }).print("No Payed");
        //6.执行任务
        env.execute();
    }
    public static class OrderReceiptKeyedProcessFunc extends KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>> {
        //声明状态
        private ValueState<OrderEvent> payEventState;
        private ValueState<TxEvent> txEventState;
        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payEventState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay-state", OrderEvent.class));
            txEventState = getRuntimeContext().getState(new ValueStateDescriptor<TxEvent>("tx-state", TxEvent.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class));
        }
        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            //取出到账状态数据
            TxEvent txEvent = txEventState.value();
            //判断到账数据是否已经到达
            if (txEvent == null) {//到账数据还没有到达
                //将自身存入状态
                payEventState.update(value);
                //注册定时器
                long ts = (value.getEventTime() + 10) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerState.update(ts);

            } else {//到账数据已经到达
                //结合写入主流
                out.collect(new Tuple2<>(value, txEvent));
                //删除定时器
                ctx.timerService().deleteEventTimeTimer(timerState.value());

                //清空状态
                txEventState.clear();
                timerState.clear();
            }
        }
        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            //取出支付数据
            OrderEvent orderEvent = payEventState.value();
            //判断支付数据是否已经到达
            if (orderEvent == null) {//支付数据没有到达
                //将自身保存至状态
                txEventState.update(value);
                //注册定时器
                long ts = (value.getEventTime() + 5) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerState.update(ts);
            } else {//支付数据已经到达
                //结合写出
                out.collect(new Tuple2<>(orderEvent, value));
                //删除定时器
                ctx.timerService().deleteEventTimeTimer(timerState.value());
                //清空状态
                payEventState.clear();
                timerState.clear();
            }
        }
        //定时器方法
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            //取出支付状态数据
            OrderEvent orderEvent = payEventState.value();
            TxEvent txEvent = txEventState.value();
            //判断orderEvent是否为Null
            if (orderEvent != null) {
                ctx.output(new OutputTag<String>("Payed No Receipt") {}, orderEvent.getTxId() + "只有支付没有到账数据");
            } else {
                ctx.output(new OutputTag<String>("Receipt No Payed") {}, txEvent.getTxId() + "只有到账没有支付数据");
            }
            //清空状态
            payEventState.clear();
            txEventState.clear();
            timerState.clear();
        }
    }
}
