package com.shangbaishuyao.demo.Function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;

public class MyWindowFunc implements WindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
        //取出迭代器中的数据
        Integer next = input.iterator().next();
        //输出数据
        out.collect(new Tuple2<>(new Timestamp(window.getStart()) + ":" + key, next));
    }
}