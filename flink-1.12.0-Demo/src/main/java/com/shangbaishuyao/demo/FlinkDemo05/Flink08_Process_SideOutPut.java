package com.shangbaishuyao.demo.FlinkDemo05;

import com.shangbaishuyao.demo.Function.SplitProcessFunc;
import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
/**
 * Author: shangbaishuyao
 * Date: 22:55 2021/4/22
 * Desc: 侧输出流
 */
public class Flink08_Process_SideOutPut {
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

        //3.使用ProcessFunction将数据分流
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.process(new SplitProcessFunc());
        //4.打印数据
        result.print("主流");
        DataStream<Tuple2<String, Integer>> sideOutput = result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("SideOut") {
        });
        sideOutput.print("Side");
        //5.执行任务
        env.execute();
    }
}
