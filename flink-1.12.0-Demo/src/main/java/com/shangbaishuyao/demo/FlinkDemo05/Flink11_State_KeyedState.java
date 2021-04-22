package com.shangbaishuyao.demo.FlinkDemo05;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
/**
 * Author: shangbaishuyao
 * Date: 22:57 2021/4/22
 * Desc:
 */
public class Flink11_State_KeyedState {
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
        //4.演示状态的使用
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new MyStateProcessFunc());
        //5.打印
        result.print();
        //6.执行任务
        env.execute();
    }
    public static class MyStateProcessFunc extends KeyedProcessFunction<String, WaterSensor, WaterSensor> {

        //a.定义状态
        private ValueState<Long> valueState;
        private ListState<Long> listState;
        private MapState<String, Long> mapState;
        private ReducingState<WaterSensor> reducingState;
        private AggregatingState<WaterSensor, WaterSensor> aggregatingState;

        //b.初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state", Long.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("list-state", Long.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state", String.class, Long.class));
//            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>())
//            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Object, WaterSensor>());
        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

            //c.状态的使用
            //c.1 Value状态
            Long value1 = valueState.value();
            valueState.update(122L);
            valueState.clear();

            //c.2 ListState
            Iterable<Long> longs = listState.get();
            listState.add(122L);
            listState.clear();
            listState.update(new ArrayList<>());

            //c.3 Map状态
            Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
            Long aLong = mapState.get("");
            mapState.contains("");
            mapState.put("", 122L);
            mapState.putAll(new HashMap<>());
            mapState.remove("");
            mapState.clear();

            //c.4 Reduce状态
            WaterSensor waterSensor = reducingState.get();
            reducingState.add(new WaterSensor());
            reducingState.clear();

            //c.5 Agg状态
            aggregatingState.add(value);
            WaterSensor waterSensor1 = aggregatingState.get();
            aggregatingState.clear();
        }
    }
}
