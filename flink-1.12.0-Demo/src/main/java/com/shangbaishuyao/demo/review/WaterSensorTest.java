package com.shangbaishuyao.demo.review;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
/*
 * Author: shangbaishuyao
 * Date: 13:54 2021/4/23
 * Desc:
 */
public class WaterSensorTest {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取Kafka数据创建流并将每行数据转换为元组,(id,1) (id,vc)
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "review");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer<String>("shangbaishuyao",
                new SimpleStringSchema(),
                properties));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sensorToOneDS = kafkaDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], 1);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sensorToVcDS = kafkaDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], Integer.parseInt(split[2]));
            }
        });

        //3.按照ID分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream1 = sensorToOneDS.keyBy(data -> data.f0);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream2 = sensorToVcDS.keyBy(data -> data.f0);

        //4.计算WordCount
        SingleOutputStreamOperator<Tuple2<String, Integer>> sensorToCountDS = keyedStream1.sum(1);

        //5.统计某个传感器连续10秒是否有水位下降,如果没有,则输出报警信息至侧输出流
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream2.process(new MyProcessFunc());

        //6.打印结果
        sensorToCountDS.print();
        result.getSideOutput(new OutputTag<String>("outPut") {
        }).print();

        //7.执行任务
        env.execute();

    }

    public static class MyProcessFunc extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private ValueState<Integer> vcState;
        private ValueState<Long> tsState;
        @Override
        public void open(Configuration parameters) throws Exception {
            vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-state", Integer.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }
        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            //获取状态数据
            Integer lastVc = vcState.value();
            Long timerTs = tsState.value();

            //获取当前数据
            Integer curVc = value.f1;

            //更新水位线状态
            vcState.update(curVc);

            //第一条数据或者定时器触发以及定时器被删除以后水位上升,注册定时器
            if (lastVc == null || (lastVc != null && curVc > lastVc)) {
                //注册定时器
                long ts = ctx.timerService().currentProcessingTime() + 10000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                //更新时间状态
                tsState.update(ts);
            } else if (timerTs != null && curVc < lastVc) {
                //删除定时器
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                //清空时间状态
                tsState.clear();
            }

            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            //输出报警信息到侧输出流
            ctx.output(new OutputTag<String>("outPut") {
            }, ctx.getCurrentKey() + "连续10秒水位没有下降");

            //清空时间状态
            tsState.clear();
        }
    }
}
