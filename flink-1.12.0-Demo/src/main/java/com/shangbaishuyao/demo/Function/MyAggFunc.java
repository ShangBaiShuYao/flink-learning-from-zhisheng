package com.shangbaishuyao.demo.Function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyAggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
        return 0;
    }
    @Override
    public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
        return accumulator + 1;
    }
    @Override
    public Integer getResult(Integer accumulator) {
        return accumulator;
    }
    @Override
    public Integer merge(Integer a, Integer b) {
        return a + b;
    }
}
