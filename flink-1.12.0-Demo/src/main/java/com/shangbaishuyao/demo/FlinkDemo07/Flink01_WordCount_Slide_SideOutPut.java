package com.shangbaishuyao.demo.FlinkDemo07;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
/**
 * Author: shangbaishuyao
 * Date: 0:33 2021/4/23
 * Desc:
 */
public class Flink01_WordCount_Slide_SideOutPut {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据创建流并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] fields = data.split(",");
                    return new WaterSensor(fields[0],
                            Long.parseLong(fields[1]),
                            Integer.parseInt(fields[2]));
                });
        //3.提取数据中的时间戳生成Watermark
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor> streamOperator = waterSensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        //4.转换为元组类型
        SingleOutputStreamOperator<Tuple2<String, Integer>> idToOneDS = streamOperator.map(new MapFunction<WaterSensor, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(WaterSensor value) throws Exception {
                return new Tuple2<>(value.getId(), 1);
            }
        });

        //5.按照ID分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = idToOneDS.keyBy(data -> data.f0);

        //6.开窗,滑动窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("SideOutPut") {
                });

        //7.计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);

        //8.打印结果
        result.print("Result");
        result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("SideOutPut") {
        }).print("Side");

        //9.执行任务
        env.execute();
    }
}
