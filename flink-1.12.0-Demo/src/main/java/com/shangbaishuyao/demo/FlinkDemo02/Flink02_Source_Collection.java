package com.shangbaishuyao.demo.FlinkDemo02;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;
import java.util.List;
/**
 * Author: shangbaishuyao
 * Date: 16:14 2021/4/22
 * Desc: 从集合中读取数据
 */
public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.准备集合
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42));
        //3.从集合读取数据
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromCollection(waterSensors);
        //4.打印
        waterSensorDataStreamSource.print();
        //5.执行任务
        env.execute();
    }
}
