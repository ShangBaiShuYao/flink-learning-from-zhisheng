package com.shangbaishuyao.demo.Function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessMapFunc extends ProcessFunction<String, Tuple2<String, Integer>> {
    @Override
    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        out.collect(new Tuple2<>(value, 1));
    }
}
