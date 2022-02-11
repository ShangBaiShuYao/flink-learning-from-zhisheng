package com.shangbaishuyao.demo.FlinkDemo07;

import com.shangbaishuyao.demo.Function.PageViewAggFunc;
import com.shangbaishuyao.demo.Function.PageViewWindowFunc;
import com.shangbaishuyao.demo.bean.PageViewCount;
import com.shangbaishuyao.demo.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;
/**
 * Author: shangbaishuyao
 * Date: 0:32 2021/4/23
 * Desc: PV
 */
public class Flink03_Practice_PageView_Window2 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.12.0-Demo/input/UserBehavior.csv");

        //3.转换为JavaBean,根据行为过滤数据,并提取时间戳生成Watermark
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = readTextFile.map(data -> {
            String[] split = data.split(",");
            return new UserBehavior(Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4]));
        }).filter(data -> "pv".equals(data.getBehavior()))
                .assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        //4.将数据转换为元组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = userBehaviorDS.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>("PV_" + new Random().nextInt(8), 1);
            }
        }).keyBy(data -> data.f0);

        //5.开窗并计算
        SingleOutputStreamOperator<PageViewCount> aggResult = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PageViewAggFunc(), new PageViewWindowFunc());

        //6.按照窗口信息重新分组做第二次聚合
        KeyedStream<PageViewCount, String> pageViewCountKeyedStream = aggResult.keyBy(PageViewCount::getTime);

        //7.累加结果
        SingleOutputStreamOperator<PageViewCount> result = pageViewCountKeyedStream.process(new PageViewProcessFunc());

        //8.执行任务
        result.print();
        env.execute();

    }

public static class PageViewProcessFunc extends KeyedProcessFunction<String, PageViewCount, PageViewCount> {
        //定义状态
        private ListState<PageViewCount> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("list-state", PageViewCount.class));
        }
        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            //将数据放入状态
            listState.add(value);
            //注册定时器
            String time = value.getTime();
            long ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();
            ctx.timerService().registerEventTimeTimer(ts + 1);
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            //取出状态中的数据
            Iterable<PageViewCount> pageViewCounts = listState.get();
            //遍历累加数据
            Integer count = 0;
            Iterator<PageViewCount> iterator = pageViewCounts.iterator();
            while (iterator.hasNext()) {
                PageViewCount next = iterator.next();
                count += next.getCount();
            }
            //输出数据
            out.collect(new PageViewCount("PV", new Timestamp(timestamp - 1).toString(), count));
            //清空状态
            listState.clear();
        }
    }
}
