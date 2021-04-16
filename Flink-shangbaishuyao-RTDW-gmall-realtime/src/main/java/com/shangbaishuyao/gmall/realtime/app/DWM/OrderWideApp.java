package com.shangbaishuyao.gmall.realtime.app.DWM;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmall.realtime.app.Function.DimAsyncFunction;
import com.shangbaishuyao.gmall.realtime.bean.OrderDetail;
import com.shangbaishuyao.gmall.realtime.bean.OrderInfo;
import com.shangbaishuyao.gmall.realtime.bean.OrderWide;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Desc: 合并订单宽表 <br/>
 *
 * TODO : 订单事实表和订单明细事实表进行双流join. intervalJoin.  难点: 事实宽表和维度表关联. 如何优化? <br/>
 *
 * 测试需要启动服务：
 * -Maxwell、zk、kafka、hdfs、hbase、BaseDBApp OrderwideApp
 * -准备配置表
 * <p>
 * 业务执行流程
 * 1.模拟生成数据jar
 * 2.在MySQL数据库的表中插入相关的业务数据
 * 3.MySQL的Binlog记录变化的数据
 * 4.Maxwell会将变化的数据采集到，并且发送到Kafka的ods_base_db_m
 * 5.BaseDBApp从ods_base_db_m主题中读取数据，进行分流操作
 * -事实数据  发送到kafka的dwd层
 * dwd_order_info
 * dwd_order_detail
 * <p>
 * -维度数据	发送到Hbase的表中
 * DIM_USER_INFO
 * 6.OrderWideApp从kafka的dwd层和Hbase的维度表中读取数据
 * -双流join
 * 使用的是intervalJoin  对OrderInfo和OrderDetail进行join
 * -维度关联
 * 旁路缓存
 * 先从Redis中查询维度，如果命中，直接获取；如果没有查询到，再到数据库中查询，
 * 并将查询的结果放到Redis中缓存起来；如果维度数据发生变化，清除缓存
 * -异步查询
 * 自定义函数类DimAsyncFunction 继承 RichAsyncFunction类
 * 从写类中的asyncInvoke，在该方法中，从线程池中获取新的线程，并执行维度关联操作
 * 模板方法设计模式：在父类中定义业务执行的模板，实现延迟到子类中完成
 * 在主程序OrderWideApp中调用函数
 * AsyncDataStream.unorderedWait(
 * stream,
 * new DimAsyncFunction(维度表名){
 * 重写getKey
 * 重写join
 * },
 * 1000,
 * TimeUnit.MILLISECONDS,
 * 100
 * );
 *
 * <p>订单和订单明细关联(双流join)</p>
 * 在flink中的流join大体分为两种，
 * 一种是基于时间窗口的join（Time Windowed Join），比如join、coGroup等。
 * 另一种是基于状态缓存的join（Temporal Table Join），比如intervalJoin。
 *
 * 这里选用intervalJoin，因为相比较窗口join，intervalJoin使用更简单，而且避免了应匹配的数据处于不同窗口的问题。
 * intervalJoin目前只有一个问题，就是还不支持left join。
 * 但是我们这里是订单主表与订单从表之间的关联不需要left join，所以intervalJoin是较好的选择。
 *
 *
 * 支付宽表的目的，最主要的原因是支付表没有到订单明细，支付金额没有细分到商品上，没有办法统计商品级的支付状况。
 * 所以本次宽表的核心就是要把支付表的信息与订单明细关联上。
 * 解决方案有两个
 * 一个是把订单明细表（或者宽表）输出到Hbase上，在支付宽表计算时查询hbase，这相当于把订单明细作为一种维度进行管理。
 * create by shangbaishuyao on 2021/4/9
 * @Author: 上白书妖
 * @Date: 15:49 2021/4/9
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1  准备本地测试流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2 设置并行度
        env.setParallelism(4);

        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"))
        //重启策略设置为不重启
//        env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 2.从Kafka的DWD层读取订单和订单明细数据
        //2.1 声明相关的主题以及消费者组
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //2.2 读取订单主题数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoJsonStrDS = env.addSource(orderInfoSource);

        //2.3 读取订单明细数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailJsonStrDS = env.addSource(orderDetailSource);

        //TODO 3.对读取的数据进行结构的转换      jsonString -->OrderInfo|OrderDetail
        //3.1 转换订单数据结构
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoJsonStrDS.map(

            new RichMapFunction<String, OrderInfo>() {
                SimpleDateFormat sdf = null;
                public void open(Configuration parameters) throws Exception {
                    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                }
                public OrderInfo map(String jsonStr) throws Exception {
                    //TODO alibaba的fastjson 将字符串妆化为我们的样例类
                    OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                    //TODO 转换时间模式
                    orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                    return orderInfo;
                }
            }
        );

        //3.2 转换订单明细数据结构
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailJsonStrDS.map(
            new RichMapFunction<String, OrderDetail>() {
                SimpleDateFormat sdf = null;
                public void open(Configuration parameters) throws Exception {
                    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                }
                public OrderDetail map(String jsonStr) throws Exception {
                    //TODO alibaba的fastjson 将字符串妆化为我们的样例类
                    OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                    //TODO 转换时间模式
                    orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                    return orderDetail;
                }
            }
        );

        //TODO 4. 指定事件时间字段
        //4.1 订单指定事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = orderInfoDS.assignTimestampsAndWatermarks(
                //TODO 指定watermark的策略,forMonotonousTimestamps()表示单调递增,forBoundedOutOfOrderness表示涉及到迟到数据.Duration.ofSeconds(3):表示整体延迟三秒钟.
            WatermarkStrategy
                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    //TODO 指定那个子段是我们的事件时间.
                    .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                        public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                            //TODO 指定那个子段是我们的事件时间.
                            return orderInfo.getCreate_ts();
                    }
                })
        );
        //4.2 订单明细指定事件时间字段
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = orderDetailDS.assignTimestampsAndWatermarks(
            WatermarkStrategy
                 .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                     .withTimestampAssigner(
                         new SerializableTimestampAssigner<OrderDetail>() {
                         public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                             return orderDetail.getCreate_ts();
                        }
                    }
                )
        );


        //TODO 做join之前,我们需要分组.
        //TODO 5.按照订单id进行分组  指定关联的key
        //OrderInfo::getId 按照订单id分组.
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithTsDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithTsDS.keyBy(OrderDetail::getOrder_id);

        //TODO 双流join做关联,orderInfoKeyedDS -----> orderInfoKeyedDS
        //TODO 6.使用intervalJoin对订单和订单明细进行关联
        //OrderWide 这张宽表,包含订单的,包含订单明细的
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
            .intervalJoin(orderDetailKeyedDS)
            .between(Time.milliseconds(-5), Time.milliseconds(5))
            .process(
                new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                }
            );

        //TODO 优化2：异步查询 <br/>
        //TODO 7.关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
            orderWideDS,
            //这里创建了一个匿名内部类. 这个匿名内部类相当于你定义了一个类.就是你定义一个类.是DimAsyncFunction的一个子类.
                //这个子类重写了他里面的额getKey方法. 我现在是多态. 我在运行的时候他会运行子类重写的方法.所以他会把geyKey获取到. 这是一个设计模式.
                //TODO 这就是一个模板方法设计模式: 在父类中指定义方法的声明,让整个流程跑通. 具体的实现延迟到子类中实现.
                //父类执行一模板.运行时子类方法会做一个重写.
            new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                //TODO 实现具体的抽闲方法.其实就是写出了具体怎么获取主键id的方法.
                public String getKey(OrderWide orderWide) {
                    return orderWide.getUser_id().toString();
                }
                //TODO 实现具体的抽象方法.
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                    //获取用户生日
                    String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                    //定义日期转换工具类
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    //将生日字符串转换为日期对象
                    Date birthdayDate = sdf.parse(birthday);
                    //获取生日日期的毫秒数
                    Long birthdayTs = birthdayDate.getTime();
                    //获取当前时间的毫秒数
                    Long curTs = System.currentTimeMillis();
                    //年龄毫秒数
                    Long ageTs = curTs - birthdayTs;
                    //转换为年龄
                    Long ageLong = ageTs / 1000L / 60L / 60L / 24L / 365L;
                    Integer age = ageLong.intValue();
                    //将维度中的年龄赋值给订单宽表中的属性
                    orderWide.setUser_age(age);
                    //将维度中的性别赋值给订单宽表中的属性
                    orderWide.setUser_gender(dimInfoJsonObj.getString("GENDER"));
                }
            },//异步请求的超时时间,1分钟
            60, TimeUnit.SECONDS);

        //TODO 8.关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
            orderWideWithUserDS,
            new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                public String getKey(OrderWide orderWide) {
                    return orderWide.getProvince_id().toString();
                }
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                    orderWide.setProvince_name(dimInfoJsonObj.getString("NAME"));
                    orderWide.setProvince_area_code(dimInfoJsonObj.getString("AREA_CODE"));
                    orderWide.setProvince_iso_code(dimInfoJsonObj.getString("ISO_CODE"));
                    orderWide.setProvince_3166_2_code(dimInfoJsonObj.getString("ISO_3166_2"));
                }
            },
            60,
            TimeUnit.SECONDS
        );

        //TODO 8.关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
            orderWideWithProvinceDS,
            new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                public String getKey(OrderWide orderWide) {
                    return orderWide.getSku_id().toString();
                }
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                    orderWide.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                    orderWide.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                    orderWide.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                    orderWide.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                }
            },
            60,
            TimeUnit.SECONDS
        );//TODO 超时的在自定义类中有一个超时处理

        //TODO 9.关联SPU商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
            orderWideWithSkuDS,
            new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                public String getKey(OrderWide orderWide) {
                    return orderWide.getSpu_id().toString();
                }
                public void join(OrderWide orderWide, JSONObject dimInfoJsonObj) throws Exception {
                    orderWide.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                }
            },
            60,
            TimeUnit.SECONDS
        );

        //TODO 10.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
            orderWideWithSpuDS,
            new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setCategory3_name(jsonObject.getString("NAME"));
                }
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getCategory3_id());
                }
            }, 60, TimeUnit.SECONDS);

        //TODO 10.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
            orderWideWithCategory3DS,
            new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                }
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getTm_id());
                }
            }, 60, TimeUnit.SECONDS);

        //TODO 11.将关联后的订单宽表数据写回到kafka的DWM层
        orderWideWithTmDS
            .map(
                orderWide->JSON.toJSONString(orderWide)
            )
            .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));
        env.execute();
    }
}
