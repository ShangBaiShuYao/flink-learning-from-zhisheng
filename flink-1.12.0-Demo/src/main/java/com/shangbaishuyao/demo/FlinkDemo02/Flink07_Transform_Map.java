package com.shangbaishuyao.demo.FlinkDemo02;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * Author: shangbaishuyao
 * Date: 16:21 2021/4/22
 * Desc:
 */
public class Flink07_Transform_Map {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文件读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile(
                "H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\flink-1.12.0-Demo\\input\\sensor.txt");
        //3.转换为JavaBean并打印数据
//        stringDataStreamSource
//                .map(new MyMapFunc())
//                .print();
        stringDataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).print();
        //4.执行
        env.execute();
    }
}
