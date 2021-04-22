package com.shangbaishuyao.demo.Function;

import com.shangbaishuyao.demo.bean.PageViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;

public class PageViewWindowFunc implements WindowFunction<Integer, PageViewCount, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<PageViewCount> out) throws Exception {
        //提取窗口时间
        String timestamp = new Timestamp(window.getEnd()).toString();
        //获取累积结果
        Integer count = input.iterator().next();
        //输出结果
        out.collect(new PageViewCount("PV", timestamp, count));
    }
}
