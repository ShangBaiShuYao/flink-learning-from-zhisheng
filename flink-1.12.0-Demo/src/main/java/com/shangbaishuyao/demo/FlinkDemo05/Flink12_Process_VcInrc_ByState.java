package com.shangbaishuyao.demo.FlinkDemo05;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
/**
 * Author: shangbaishuyao
 * Date: 22:58 2021/4/22
 * Desc:
 */
public class Flink12_Process_VcInrc_ByState {
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
        //4.使用ProcessFunction实现连续时间内水位不下降,则报警,且将报警信息输出到侧输出流
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            //定义状态
            private ValueState<Integer> vcState;
            private ValueState<Long> tsState;
            //初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-state", Integer.class, Integer.MIN_VALUE));
                tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
            }
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //取出状态数据
                Integer lastVc = vcState.value();
                Long timerTs = tsState.value();
                //取出当前数据中的水位线
                Integer curVc = value.getVc();
                //当水位上升并且timerTs为NULL的时候
                if (curVc >= lastVc && timerTs == null) {
                    //注册定时器
                    long ts = ctx.timerService().currentProcessingTime() + 10000L;
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    tsState.update(ts);
                } else if (curVc < lastVc && timerTs != null) {
                    //删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                    tsState.clear();
                }
                //更新上一次水位线的状态
                vcState.update(curVc);
                //输出数据
                out.collect(value);
            }
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {

                ctx.output(new OutputTag<String>("SideOutPut") {
                           },
                        ctx.getCurrentKey() + "连续10秒没有下降！");

                //清空定时器时间状态
                tsState.clear();
            }
        });
        //5.打印数据
        result.print("主流");
        result.getSideOutput(new OutputTag<String>("SideOutPut") {
        }).print("SideOutPut");
        //6.执行任务
        env.execute();
    }
}
