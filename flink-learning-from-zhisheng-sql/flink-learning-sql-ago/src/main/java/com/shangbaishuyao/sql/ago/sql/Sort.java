package com.shangbaishuyao.sql.ago.sql;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.time.Instant;
import java.util.Random;

/**
 * Desc: 使用 Table / SQL API 根据事件时间和水印对无序流进行排序 <br/>
 *
 * create by shangbaishuyao on 2021/2/1
 * @Author: 上白书妖
 * @Date: 18:06 2021/2/1
 */
public class Sort {
    public static final int OUT_OF_ORDERNESS = 1000;
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


    }

    public static class Event {
        Long eventTime;
        Event() {
            //构造生成带有事件时间的数据(乱序)
            this.eventTime = Instant.now().toEpochMilli() + (new Random().nextInt(OUT_OF_ORDERNESS));
        }
        @Override
        public String toString() {
            return "Event{" + "eventTime=" + eventTime + '}';
        }
    }


    /**
     * 数据源,此处不断造数据
     */
    private static class OutOfOrderEventSource extends RichSourceFunction<Event>{

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                ctx.collect(new Event());
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * 时间水印
     */
    private static class TimestampsAndWatermarks extends BoundedOutOfOrdernessTimestampExtractor<Event> {

        public TimestampsAndWatermarks() {
            super(Time.milliseconds(OUT_OF_ORDERNESS));
        }

        @Override
        public long extractTimestamp(Event event) {
            return event.eventTime;
        }
    }
}
