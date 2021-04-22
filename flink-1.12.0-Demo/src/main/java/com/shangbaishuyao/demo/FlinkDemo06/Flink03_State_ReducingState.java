package com.shangbaishuyao.demo.FlinkDemo06;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
/**
 * Author: shangbaishuyao
 * Date: 0:20 2021/4/23
 * Desc:
 */
public class Flink03_State_ReducingState {
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
        //3.按照传感器ID分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(WaterSensor::getId);
        //4.使用状态编程的方式实现累加传感器的水位线
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            //定义状态
            private ReducingState<WaterSensor> reducingState;
            @Override
            public void open(Configuration parameters) throws Exception {
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>("reduce", new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                    }
                }, WaterSensor.class));
            }
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //将当前数据聚合进状态
                reducingState.add(value);
                //取出状态中的数据
                WaterSensor waterSensor = reducingState.get();
                //输出数据
                out.collect(waterSensor);
            }
        }).print();
        //5.执行任务
        env.execute();
    }
}
