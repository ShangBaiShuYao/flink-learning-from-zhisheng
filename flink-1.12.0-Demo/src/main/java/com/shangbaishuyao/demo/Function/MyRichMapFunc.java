package com.shangbaishuyao.demo.Function;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

//RichFunction富有的地方在于:1.声明周期方法,2.可以获取上下文执行环境,做状态编程
public class MyRichMapFunc extends RichMapFunction<String, WaterSensor> {
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("Open方法被调用！！");
    }
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] split = value.split(",");
        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
    }
    @Override
    public void close() throws Exception {
        System.out.println("Close方法被调用！！！");
    }
}
