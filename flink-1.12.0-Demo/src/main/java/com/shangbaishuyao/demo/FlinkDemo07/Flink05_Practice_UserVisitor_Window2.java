package com.shangbaishuyao.demo.FlinkDemo07;

import com.shangbaishuyao.demo.bean.UserBehavior;
import com.shangbaishuyao.demo.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
/**
 * Author: shangbaishuyao
 * Date: 0:33 2021/4/23
 * Desc: UV
 */
public class Flink05_Practice_UserVisitor_Window2 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile("input/UserBehavior.csv");
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
        //4.按照行为分组
        KeyedStream<UserBehavior, String> keyedStream = userBehaviorDS.keyBy(UserBehavior::getBehavior);
        //5.开窗
        WindowedStream<UserBehavior, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)));
        //6.使用布隆过滤器  自定义触发器:来一条计算一条(访问Redis一次)
        SingleOutputStreamOperator<UserVisitorCount> result = windowedStream
                .trigger(new MyTrigger())
                .process(new UserVisitorWindowFunc());
        //7.打印并执行任务
        result.print();
        env.execute();
    }

//自定义触发器:来一条计算一条(访问Redis一次)
public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {}
    }

public static class UserVisitorWindowFunc extends ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow> {
        //声明Redis连接
        private Jedis jedis;
        //声明布隆过滤器
        private MyBloomFilter myBloomFilter;
        //声明每个窗口总人数的Key
        private String hourUvCountKey;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("hadoop102", 6379);
            hourUvCountKey = "HourUv";
            myBloomFilter = new MyBloomFilter(1 << 30);
        }
        @Override
        public void process(String key, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {
            //1.取出数据
            UserBehavior userBehavior = elements.iterator().next();
            //2.提取窗口信息
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            //3.定义当前窗口的BitMap Key
            String bitMapKey = "BitMap_" + windowEnd;
            //4.查询当前的UID是否已经存在于当前的bitMap中
            long offset = myBloomFilter.getOffset(userBehavior.getUserId().toString());
            Boolean exist = jedis.getbit(bitMapKey, offset);
            //5.根据数据是否存在决定下一步操作
            if (!exist) {
                //将对应Offset位置改为1
                jedis.setbit(bitMapKey, offset, true);
                //累加当前窗口的总和
                jedis.hincrBy(hourUvCountKey, windowEnd, 1);
            }
            //输出数据
            String hget = jedis.hget(hourUvCountKey, windowEnd);
            out.collect(new UserVisitorCount("UV", windowEnd, Integer.parseInt(hget)));
        }
    }

//自定义布隆过滤器
public static class MyBloomFilter {
        //定义布隆过滤器容量,最好传入2的整次幂数据
        private long cap;
        public MyBloomFilter(long cap) {
            this.cap = cap;
        }
        //传入一个字符串,获取在BitMap中的位置
        public long getOffset(String value) {
            long result = 0L;
            for (char c : value.toCharArray()) {
                result += result * 31 + c;
            }
            //取模
            return result & (cap - 1);
        }
    }
}
