package com.shangbaishuyao.demo.FlinkDemo06;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
/**
 * Key-Value(键控状态)也是有所谓的数据结构的.它存储的数据结构呢有这么几种:
 * 第一种是ValueState[T],保存单个的值,这里面我需要指定一个名字和一个类型,而T就是类型.我们在初始化的时候一定要给ValueState[T]一个名字.这个名字实际上就是所谓的键.当然这种情况下就是我们自己往里面放的.这个键很明显是我们自己定义的.跟我们流里面的数据可能没有任何关系.
 * 第二种是ListState[T],保存一个列表,既然是列表,那我们可以往里面添加,也可以添加多个,添加一个集合进去,可以返回,可以修改.
 * 第三种是MapState[T],保存Key-value对.就是把我们的数据存成多个.假如你的数据有多个键的话,最好存储的时候存成MapState[T].实际上ListState[T]不太适合,为什么呢?因为ListState[T]他实际上只有一个名字(键),然后这个名字(键)里面有多个值.多个值组成一个List集合.如果我的数据是一个键对应一个值,一个键对应一个值的话.这种情况下我应该采用MapState[T].
 * 第四种ReducingState[T]和第五种AggregatingState[I,O],这两个都是用在做增量聚合的时候的一个状态.
 * 最后就是State.Clear()的一个清空操作.
 *
 * Author: shangbaishuyao
 * Date: 0:21 2021/4/23
 * Desc:
 */
public class Flink05_State_MapState {
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
        //4.使用状态编程方式实现水位线去重
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            //定义状态
            private MapState<Integer, String> mapState;
            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, String>("map-state", Integer.class, String.class));
            }
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //取出当前数据中的水位线
                Integer vc = value.getVc();
                //判断状态中是否包含当前的水位值
                if (!mapState.contains(vc)) {
                    out.collect(value);
                    mapState.put(vc, "aa");
                }
            }
        }).print();
        //5.执行任务
        env.execute();
    }
}
