package com.shangbaishuyao.demo.FlinkDemo05;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;
/**
 * Author: shangbaishuyao
 * Date: 22:54 2021/4/22
 * Desc: 滑动窗口
 */
public class Flink03_Window_EventTimeSliding {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });
        //3.提取数据中的时间戳字段
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        //4.按照id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);
        //5.开窗
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(2)));
        //6.计算总和
        SingleOutputStreamOperator<WaterSensor> result = window.sum("vc");
        //7.打印
        result.print();
        //8.执行任务
        env.execute();
    }
}
