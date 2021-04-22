package com.shangbaishuyao.demo.FlinkDemo04;

import com.shangbaishuyao.demo.Function.AppMarketingDataSource;
import com.shangbaishuyao.demo.bean.MarketingUserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
/**
 * 市场营销商业指标统计分析:
 * 随着智能手机的普及，在如今的电商网站中已经有越来越多的用户来自移动端，相比起传统浏览器的登录方式，手机APP成为了更多用户访问电商网站的首选。
 * 对于电商企业来说，一般会通过各种不同的渠道对自己的APP进行市场推广，而这些渠道的统计数据（比如，不同网站上广告链接的点击量、APP下载量）就成了市场营销的重要商业指标。
 * Author: shangbaishuyao
 * Date: 18:01 2021/4/22
 * Desc: APP市场推广统计 - 分渠道
 */
public class Flink06_Practice_MarketByChannel {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.从自定义Source中加载数据
        DataStreamSource<MarketingUserBehavior> marketingUserBehaviorDS = env.addSource(new AppMarketingDataSource());
        //3.按照渠道以及行为分组
        KeyedStream<MarketingUserBehavior, Tuple2<String, String>> keyedStream = marketingUserBehaviorDS.keyBy(
                new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<>(value.getChannel(), value.getBehavior());
                    }
                });
        //4.计算总和
        SingleOutputStreamOperator<Tuple2<Tuple2<String, String>, Integer>> result = keyedStream.process(new KeyedProcessFunction<Tuple2<String, String>, MarketingUserBehavior, Tuple2<Tuple2<String, String>, Integer>>() {
            private HashMap<String, Integer> hashMap = new HashMap<>();
            @Override
            public void processElement(MarketingUserBehavior value, Context ctx, Collector<Tuple2<Tuple2<String, String>, Integer>> out) throws Exception {
                //拼接HashKey
                String hashKey = value.getChannel() + "-" + value.getBehavior();
                //取出HashMap中的数据,如果该数据是第一次过来,则给定默认值0
                Integer count = hashMap.getOrDefault(hashKey, 0);
                count++;
                //输出结果
                out.collect(new Tuple2<>(ctx.getCurrentKey(), count));
                //更新HashMap中的数据
                hashMap.put(hashKey, count);
            }
        });
        //5.打印数据
        result.print();
        //6.执行任务
        env.execute();
    }
}
