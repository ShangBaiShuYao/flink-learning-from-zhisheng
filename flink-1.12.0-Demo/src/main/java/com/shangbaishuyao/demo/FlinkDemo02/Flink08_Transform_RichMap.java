package com.shangbaishuyao.demo.FlinkDemo02;

import com.shangbaishuyao.demo.Function.MyRichMapFunc;
import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_Transform_RichMap {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2.从文件读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\flink-1.12.0-Demo\\input\\sensor.txt");
        //3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = stringDataStreamSource.map(new MyRichMapFunc());
        //4.打印结果数据
        waterSensorDS.print();
        //5.执行任务
        env.execute();
    }
}
