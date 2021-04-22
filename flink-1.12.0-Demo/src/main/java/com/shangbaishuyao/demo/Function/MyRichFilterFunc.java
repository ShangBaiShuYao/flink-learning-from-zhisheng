package com.shangbaishuyao.demo.Function;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

public class MyRichFilterFunc extends RichFilterFunction<String> {
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("aaaa");
    }
    @Override
    public boolean filter(String value) throws Exception {
        String[] split = value.split(",");
        return Integer.parseInt(split[2]) > 30;
    }
}
