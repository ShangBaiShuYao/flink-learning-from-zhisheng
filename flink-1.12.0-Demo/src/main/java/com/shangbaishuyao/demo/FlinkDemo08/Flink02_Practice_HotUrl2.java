package com.shangbaishuyao.demo.FlinkDemo08;

import com.shangbaishuyao.demo.bean.ApacheLog;
import com.shangbaishuyao.demo.bean.UrlCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
/**
 * Author: shangbaishuyao
 * Date: 12:33 2021/4/23
 * Desc: 基于服务器log的热门页面浏览量统计
 */
public class Flink02_Practice_HotUrl2 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取文本数据创建流并转换为JavaBean对象提取时间戳生成WaterMark
        WatermarkStrategy<ApacheLog> apacheLogWatermarkStrategy = WatermarkStrategy.<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                    @Override
                    public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                        return element.getTs();
                    }
                });
//        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.readTextFile("H:\IDEA_WorkSpace\flink-learning-from-zhisheng\flink-1.12.0-Demo\input\\apache.log")
        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(" ");
                    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    return new ApacheLog(split[0],
                            split[1],
                            sdf.parse(split[3]).getTime(),
                            split[5],
                            split[6]);
                }).filter(data -> "GET".equals(data.getMethod()))
                .assignTimestampsAndWatermarks(apacheLogWatermarkStrategy);
        //3.转换为元组,（url,1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> urlToOneDS = apacheLogDS.map(new MapFunction<ApacheLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(ApacheLog value) throws Exception {
                return new Tuple2<>(value.getUrl(), 1);
            }
        });
        //4.按照Url分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = urlToOneDS.keyBy(data -> data.f0);
        //5.开窗聚合,增量 + 窗口函数
        SingleOutputStreamOperator<UrlCount> urlCountByWindowDS = keyedStream
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {
                })
                .aggregate(new HotUrlAggFunc(), new HotUrlWindowFunc());
        //6.按照窗口信息重新分组
        KeyedStream<UrlCount, Long> windowEndKeyedStream = urlCountByWindowDS.keyBy(UrlCount::getWindowEnd);
        //7.使用状态编程+定时器的方式,实现同一个窗口中所有数据的排序输出
        SingleOutputStreamOperator<String> result = windowEndKeyedStream.process(new HotUrlProcessFunc(5));
        //8.打印数据
        apacheLogDS.print("apacheLogDS");
        urlCountByWindowDS.print("urlCountByWindowDS");
        result.print("Result");
        //9.执行任务
        env.execute();
    }
    public static class HotUrlAggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
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
    public static class HotUrlWindowFunc implements WindowFunction<Integer, UrlCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow window, Iterable<Integer> input, Collector<UrlCount> out) throws Exception {
            Integer count = input.iterator().next();
            out.collect(new UrlCount(url, window.getEnd(), count));
        }
    }
    public static class HotUrlProcessFunc extends KeyedProcessFunction<Long, UrlCount, String> {
        private Integer topSize;
        public HotUrlProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }
        //声明状态
        private MapState<String, UrlCount> mapState;
        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, UrlCount>("map-state", String.class, UrlCount.class));
        }
        @Override
        public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {
            //将当前数据放置状态
            mapState.put(value.getUrl(), value);
            //注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
            //注册定时器,在窗口真正关闭以后,用于清空状态的
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 61001L);
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (timestamp == ctx.getCurrentKey() + 61001L) {
                mapState.clear();
                return;
            }
            //取出状态中的数据
            Iterator<Map.Entry<String, UrlCount>> iterator = mapState.iterator();
            ArrayList<Map.Entry<String, UrlCount>> entries = Lists.newArrayList(iterator);
            //排序
            entries.sort((o1, o2) -> o2.getValue().getCount() - o1.getValue().getCount());
            //取TopN数据
            StringBuilder sb = new StringBuilder();
            sb.append("===========")
                    .append(new Timestamp(timestamp - 1L))
                    .append("===========")
                    .append("\n");
            for (int i = 0; i < Math.min(topSize, entries.size()); i++) {
                UrlCount urlCount = entries.get(i).getValue();

                sb.append("Top").append(i + 1);
                sb.append(" Url:").append(urlCount.getUrl());
                sb.append(" Count:").append(urlCount.getCount());
                sb.append("\n");
            }
            sb.append("===========")
                    .append(new Timestamp(timestamp - 1L))
                    .append("===========")
                    .append("\n")
                    .append("\n");
            //1输出数据
            out.collect(sb.toString());
            //休息一会
            Thread.sleep(2000);
        }
    }
}
