package com.shangbaishuyao.demo.FlinkDemo08;

import com.shangbaishuyao.demo.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
/**
 * Author: shangbaishuyao
 * Date: 12:37 2021/4/23
 * Desc: 恶意登录监控
 */
public class Flink05_Practice_LoginApp2 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取文本数据,转换为JavaBean,提取时间戳生成Watermark
        WatermarkStrategy<LoginEvent> loginEventWatermarkStrategy = WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.12.0-Demo/input/LoginLog.csv")
                .map(data -> {
                    String[] split = data.split(",");
                    return new LoginEvent(Long.parseLong(split[0]),
                            split[1],
                            split[2],
                            Long.parseLong(split[3]));
                }).assignTimestampsAndWatermarks(loginEventWatermarkStrategy);

        //3.按照用户ID分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);
        //4.使用ProcessAPI,状态,定时器
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginKeyedProcessFunc(2));
        //5.打印结果
        result.print();
        //6.执行任务
        env.execute();
    }

    public static class LoginKeyedProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {
        //定义属性信息
        private Integer ts;
        public LoginKeyedProcessFunc(Integer ts) {
            this.ts = ts;
        }
        //声明状态
        private ValueState<LoginEvent> failEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            failEventState = getRuntimeContext().getState(new ValueStateDescriptor<LoginEvent>("fail-state", LoginEvent.class));
        }
        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            //判断数据类型
            if ("fail".equals(value.getEventType())) {
                //取出状态中的数据
                LoginEvent loginEvent = failEventState.value();
                //更新状态
                failEventState.update(value);
                //如果为非第一条失败数据并且时间间隔小于等于ts值,则输出报警信息
                if (loginEvent != null && Math.abs(value.getEventTime() - loginEvent.getEventTime()) <= ts) {
                    //输出报警信息
                    out.collect(value.getUserId() + "连续登录失败2次！");
                }
            } else {
                //成功数据,清空状态
                failEventState.clear();
            }
        }
    }
}
