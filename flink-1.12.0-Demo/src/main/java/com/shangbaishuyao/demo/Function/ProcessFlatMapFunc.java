package com.shangbaishuyao.demo.Function;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFlatMapFunc extends ProcessFunction<String, String> {
    //生命周期方法
    @Override
    public void open(Configuration parameters) throws Exception {
    }
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        //运行时上下文,状态编程
        RuntimeContext runtimeContext = getRuntimeContext();
        String[] words = value.split(" ");
        for (String word : words) {
            out.collect(word);
        }
        //定时器
        TimerService timerService = ctx.timerService();
        timerService.registerProcessingTimeTimer(1245L);
        //获取当前处理数据的时间
        timerService.currentProcessingTime();
        timerService.currentWatermark();  //事件时间
        //侧输出流
        //ctx.output();
    }
}

