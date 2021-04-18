package com.shangbaishuyao.gmall.realtime.app.DWS;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.shangbaishuyao.gmall.realtime.app.Function.DimAsyncFunction;
import com.shangbaishuyao.gmall.realtime.bean.OrderWide;
import com.shangbaishuyao.gmall.realtime.bean.PaymentWide;
import com.shangbaishuyao.gmall.realtime.bean.ProductStats;
import com.shangbaishuyao.gmall.realtime.common.GmallConstant;
import com.shangbaishuyao.gmall.realtime.utils.ClickHouseUtil;
import com.shangbaishuyao.gmall.realtime.utils.DateTimeUtil;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Author: shangbaishuyao
 * Date: 2021/2/23
 * Desc: 商品主题统计应用
 * 执行前需要启动的服务
 *    -zk,kafka,logger.sh(nginx + 日志处理服务),maxwell,hdfs,hbase,Redis,ClichHouse
 *    -BaseLogApp,BaseDBApp,OrderWideApp,PaymentWide,ProductStatsApp
 * 执行流程
 *
 *  DWS层-商品主题宽表的计算
 *  统计主题:商品主题
 *
 *  需求指标	    输出方式     计算来源	        来源层级
 *  点击	    多维分析	    page_log直接可求	dwd
 * 	曝光	    多维分析	    page_log直接可求	dwd
 * 	收藏	    多维分析	    收藏表	        dwd
 * 	加入购物车	多维分析	    购物车表	        dwd
 * 	下单	    可视化大屏	订单宽表	        dwm
 * 	支付	    多维分析	    支付宽表        	dwm
 * 	退款	    多维分析	    退款表	        dwd
 * 	评价	    多维分析	    评价表	        dwd
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","shangbaishuyao");
        */

        //TODO 2.从Kafka中获取数据流
        //2.1 声明相关的主题名称以及消费者组
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //2.2 从页面日志中获取点击和曝光数据
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);

        //2.3 从dwd_favor_info中获取收藏数据
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);

        //2.4 从dwd_cart_info中获取购物车数据
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);

        //2.5 从dwm_order_wide中获取订单数据
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);

        //2.6 从dwm_payment_wide中获取支付数据
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);

        //2.7 从dwd_order_refund_info中获取退款数据
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);

        //2.8 从dwd_order_refund_info中获取评价数据
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);


        //TODO 3.将各个流的数据转换为统一的对象格式
        //3.1 对点击和曝光数据进行转换      jsonStr-->ProduceStats
        SingleOutputStreamOperator<ProductStats> productClickAndDispalyDS = pageViewDStream.process(
            new ProcessFunction<String, ProductStats>() {
                @Override
                public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                    //将json格式字符串转换为json对象
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    String pageId = pageJsonObj.getString("page_id");
                    if (pageId == null) {
                        System.out.println("将json格式字符串转换为json对象:" + jsonObj);
                    }
                    //获取操作时间
                    Long ts = jsonObj.getLong("ts");
                    //如果当前访问的页面是商品详情页，认为该商品被点击了一次
                    if ("good_detail".equals(pageId)) {
                        //获取被点击商品的id
                        Long skuId = pageJsonObj.getLong("item");
                        //封装一次点击操作
                        ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1L).ts(ts).build();
                        //向下游输出
                        out.collect(productStats);
                    }

                    JSONArray displays = jsonObj.getJSONArray("displays");
                    //如果displays属性不为空，那么说明有曝光数据
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            //获取曝光数据
                            JSONObject displayJsonObj = displays.getJSONObject(i);
                            //判断是否曝光的某一个商品
                            if ("sku_id".equals(displayJsonObj.getString("item_type"))) {
                                //获取商品id
                                Long skuId = displayJsonObj.getLong("item");
                                //封装曝光商品对象
                                ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                                //向下游输出
                                out.collect(productStats);
                            }
                        }
                    }
                }
            }
        );
        //TODO 灵魂发问: 为什么这里不赋值维度的值呢?而只是赋值度量的值. 这里可不可以赋维度的值呢?
        //可以, 我赋值的话,只有其中一个流的维度值有数据,其他流没有. 但是这个是没有关系的. 因为我下面还有四流的合并.也要做一次维度的关联
        //所以我们在这里只做度量的赋值,不做维度的赋值. 因为最后我们还有四流的合并. 在这里做维度赋值意义不大.
        //那如果我这里不做关联. 我光从订单宽表里面读数据,我们的维度关联是不是就没用了呢?
        //不是的. 首先你要明白. 我们DWM层是干嘛的. DWM层的定位是什么?
        //DWM层的定位是什么，DWM层主要服务DWS，因为部分需求直接从DWD层到DWS层中间会有一定的计算量，而且这部分计算的结果很有可能被多个DWS层主题复用，所以部分DWD成会形成一层DWM.
        //3.2 对订单宽表进行转换      jsonStr-->ProductStats
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDStream.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    //将json字符串转换为对应的订单宽表对象
                    OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                    String create_time = orderWide.getCreate_time();
                    //将字符串日期转换为毫秒数
                    Long ts = DateTimeUtil.toTs(create_time);
                    ProductStats productStats = ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .ts(ts)
                        .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                        .build();
                    return productStats;
                }
            }
        );

        //3.3转换收藏流数据
        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorInfoDStream.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    //将字符串日期转换为毫秒数
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(ts)
                        .build();
                    return productStats;
                }
            }
        );

        //3.4转换购物车流数据
        SingleOutputStreamOperator<ProductStats> cartStatsDS = cartInfoDStream.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    //将字符串日期转换为毫秒数
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));

                    ProductStats productStats = ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .cart_ct(1L)
                        .ts(ts)
                        .build();
                    return productStats;
                }
            }
        );

        //3.5转换支付流数据
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDStream.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonObj) throws Exception {
                    PaymentWide paymentWide = JSON.parseObject(jsonObj, PaymentWide.class);
                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                    return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                        .ts(ts)
                        .build();
                }
            }
        );

        //3.6转换退款流数据
        SingleOutputStreamOperator<ProductStats> refundStatsDS= refundInfoDStream.map(
            jsonStr -> {
                JSONObject refundJsonObj = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                ProductStats productStats = ProductStats.builder()
                    .sku_id(refundJsonObj.getLong("sku_id"))
                    .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(
                        new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                    .ts(ts)
                    .build();
                return productStats;
            });

        //3.7转换评价流数据
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDS= commentInfoDStream.map(
            jsonStr -> {
                JSONObject commonJsonObj = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                ProductStats productStats = ProductStats.builder()//builder获取内部类
                    .sku_id(commonJsonObj.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(ts)
                    .build();//build获取外部类. 中间给属性赋值.
                return productStats;
            });

        //TODO 4. 将转换后的流进行合并
        DataStream<ProductStats> unionDS = productClickAndDispalyDS.union(
            orderWideStatsDS,
            favorStatsDS,
            cartStatsDS,
            paymentStatsDS,
            refundStatsDS,
            commonInfoStatsDS
        );

        //TODO 5.设置Watermark并且提取事件时间字段
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<ProductStats>forMonotonousTimestamps()
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<ProductStats>() {
                        @Override
                        public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                            return productStats.getTs();
                        }
                    }
                )
        );

        //TODO 6.按照维度对数据进行分组
        KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(
            new KeySelector<ProductStats, Long>() {
                @Override
                public Long getKey(ProductStats productStats) throws Exception {
                    return productStats.getSku_id();
                }
            }
        );

        //TODO 7.对分组之后的数据进行开窗   开一个10s的滚动窗口
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(
            TumblingEventTimeWindows.of(Time.seconds(10))
        );

        //TODO 8.对窗口中的元素进行聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
            new ReduceFunction<ProductStats>() {
                @Override
                public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                    stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                    stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                    stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                    stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                    stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                    stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                    stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                    stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                    stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                    stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                    stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                    stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                    stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                    stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                    stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                    stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                    return stats1;
                }
            },
            new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                @Override
                public void process(Long key, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    for (ProductStats productStats : elements) {
                        productStats.setStt(simpleDateFormat.format(new Date(context.window().getStart())));
                        productStats.setEdt(simpleDateFormat.format(new Date(context.window().getEnd())));
                        productStats.setTs(new Date().getTime());
                        out.collect(productStats);
                    }
                }
            }

        );

//------------------------------到此为止,没用做维度关联,只是对可度量的值进行相加.只有商品的id,没用具体的商品名称,商品的品牌,品类都不知道,所下面需要做维度关联,从Hbase中获取具体信息----------------------------------------

        //TODO 9.补充商品的维度信息  :  unorderedWait
        //9.1 关联商品维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
            reduceDS,
            new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                @Override
                public String getKey(ProductStats productStats) {
                    return productStats.getSku_id().toString();
                }
                @Override
                public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                    productStats.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                    productStats.setSku_price(dimInfoJsonObj.getBigDecimal("PRICE"));
                    productStats.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                    productStats.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                    productStats.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        //9.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(
            productStatsWithSkuDS,
            new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                @Override
                public String getKey(ProductStats productStats) {
                    return productStats.getSpu_id().toString();
                }

                @Override
                public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                    productStats.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );

        //9.3 关联品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTMDS = AsyncDataStream.unorderedWait(
            productStatsWithSpuDS,
            new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                @Override
                public String getKey(ProductStats productStats) {
                    return productStats.getTm_id().toString();
                }

                @Override
                public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                    productStats.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );

        //9.4 关联品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategoryDS = AsyncDataStream.unorderedWait(
            productStatsWithTMDS,
            new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                @Override
                public String getKey(ProductStats productStats) {
                    return productStats.getCategory3_id().toString();
                }
                @Override
                public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                    productStats.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );

        //TODO 10.将聚合后的流数据写到ClickHouse中
        productStatsWithCategoryDS.addSink(
            ClickHouseUtil.<ProductStats>getJdbcSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                ));

        //TODO 11.将统计的结果写回到kafka的dws层
        productStatsWithCategoryDS
            .map(productStat->JSON.toJSONString(productStat,new SerializeConfig(true)))
            .addSink(MyKafkaUtil.getKafkaSink("dws_product_stats"));
        env.execute();
    }
}
