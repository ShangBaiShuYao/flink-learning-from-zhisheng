package com.shangbaishuyao.demo.Function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class MyRichFlatMapFunc extends RichFlatMapFunction<String, String> {
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("aaa");
    }
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] split = value.split(",");
        for (String s : split) {
            out.collect(s);
        }
    }
}
