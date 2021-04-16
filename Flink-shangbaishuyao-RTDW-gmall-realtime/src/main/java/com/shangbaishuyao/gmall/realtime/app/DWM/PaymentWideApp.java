package com.shangbaishuyao.gmall.realtime.app.DWM;

import com.alibaba.fastjson.JSON;
import com.shangbaishuyao.gmall.realtime.bean.OrderWide;
import com.shangbaishuyao.gmall.realtime.bean.PaymentInfo;
import com.shangbaishuyao.gmall.realtime.bean.PaymentWide;
import com.shangbaishuyao.gmall.realtime.utils.DateTimeUtil;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Desc: 支付宽表处理程序 <br/>
 *
 * create by shangbaishuyao on 2021/4/9
 * @Author: 上白书妖
 * @Date: 15:49 2021/4/9
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境的准备
        //1.1 创建流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //1.3 检查点相关的配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/paymentWide"));
        */

        //TODO 2. 从kafka的主题中读取数据
        //2.1 声明相关的主题以及消费者组
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        String groupId = "paymentwide_app_group";

        //2.1 读取支付数据
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        DataStreamSource<String> paymentInfoJsonStrDS = env.addSource(paymentInfoSource);

        //2.2 读取订单宽表数据
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideJsonStrDS = env.addSource(orderWideSource);

        //TODO 3. 对读取到的数据进行结构的转换   jsonStr->POJO
        //3.1 转换支付流
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoJsonStrDS.map(
            jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class)
        );
        //3.2 转换订单流
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideJsonStrDS.map(
            jsonStr -> JSON.parseObject(jsonStr, OrderWide.class)
        );

        //TODO 4.设置Watermark以及提取事件时间字段
        //4.1 支付流的Watermark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<PaymentInfo>() {
                        @Override
                        public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                            //需要将字符串的时间转换为毫秒数
                            return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                        }
                    }
                )
        );

        //4.2 订单流的Watermark
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS = orderWideDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<OrderWide>() {
                        @Override
                        public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                            return DateTimeUtil.toTs(orderWide.getCreate_time());
                        }
                    }
                )
        );

        //TODO 5.对数据进行分组
        //5.1 支付流数据分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(PaymentInfo::getOrder_id);
        //5.2 订单宽表流数据分组
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS.keyBy(OrderWide::getOrder_id);

        //TODO 6.使用IntervalJoin关联两条流
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS
            .intervalJoin(orderWideKeyedDS)
            .between(Time.seconds(-1800), Time.seconds(0))//找支付和订单半小时之内的数据
            .process(
                new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                }
            );

        //TODO 7.将数据写到kafka的dwm层
        paymentWideDS.map(
            paymentWide->JSON.toJSONString(paymentWide)
        ).addSink(
            MyKafkaUtil.getKafkaSink(paymentWideSinkTopic)
        );
        env.execute();
    }
}
