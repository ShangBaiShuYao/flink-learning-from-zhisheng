package com.shangbaishuyao.gmall.realtime.app.DWS;

import com.shangbaishuyao.gmall.realtime.bean.ProvinceStats;
import com.shangbaishuyao.gmall.realtime.utils.ClickHouseUtil;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: shangbaishuyao
 * Date: 2021/2/24
 * Desc: 使用FlinkSQL对地区主题统计
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1. 基本环境准备
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
        //1.4 创建Table环境
        EnvironmentSettings setting = EnvironmentSettings
            .newInstance()
            .inStreamingMode()//设置流模式，不是批模式。 Flink虽然可以流批一体。
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //TODO 2.把数据源定义为动态表
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        //如何把kafka的数据读取到我们的Flink SQL里面来呢? 是由一个动态表的概念
        tableEnv.executeSql(
                //创建动态表
                "CREATE TABLE ORDER_WIDE " +
                        "(province_id BIGINT, " +
                        "province_name STRING," +
                        "province_area_code STRING" +
                        ",province_iso_code STRING," +
                        "province_3166_2_code STRING," +
                        "order_id STRING, " +
                        "split_total_amount DOUBLE," +
                        "create_time STRING," +
                        "rowtime AS TO_TIMESTAMP(create_time) ," +  //转换时间戳
                        //watermark定义表的事件时间属性, 他的形式就是这种形式
                        "WATERMARK FOR  rowtime  AS rowtime)" +
                 //with里面放的就是kafka的连接信息.包括你连接的是谁,kafka,kafka的那个主题.kafka的地址,指定消费者组,从什么地方开始消费,处理的格式
                 " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        //我从kafka的DWM层的订单宽表拿到所有数据.拿到之后我想做一个统计. 统计按照什么维度呢?地区.
        //既然我按照订单的维度按照订单的信息做一个统计的话,那我是不是应该按照不同的维度来进行分组呢. 这个地区是什么,那个地区是什么.... ,我要统计这个地区哪个时间段的什么情况,那个地区这个时间段的又是什么情况.
        //所以我要做两件事. ①分组 (按照维度:地区) ②开窗
        //有了表了之后,可以从表里面查询了.
        //TODO 3.聚合计算
        Table provinceStateTable = tableEnv.sqlQuery(
                "select " +
                        //获取当前窗口的起始时间  rowtime:表示事件时间字段
                   "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                        //获取当前窗口的结束时间
                   "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
//-----------------------------------------------------------维度数据----------------------------------------------------------------
                        //省份id
                   "province_id," +
                        //省份名称
                   "province_name," +
                        //省份的地区编码
                   "province_area_code " +
                        //地区编码
                   "area_code," +
                   "province_iso_code iso_code ," +
                   "province_3166_2_code iso_3166_2 ," +
                        //统计下了多少订单. 因为订单和订单明细这两个事实表已经做了一个关联了. 一个订单可能对应多个明细
                        //所以想要统计订单数,你每过来一个都要统计,这是不合适的.所以要做一个去重操作
                   "COUNT( DISTINCT  order_id) order_count," + //TODO 这是对当前窗口的数据做一个聚合操作
                        //当前订单的金额
                   "sum(split_total_amount) order_amount," +   //TODO 这是对当前窗口的数据做一个聚合操作
//-----------------------------------------------------------维度数据---------------------------------------------------------------
                        //当前系统时间乘以1000 获取秒转化成毫秒
                   "UNIX_TIMESTAMP()*1000 ts "+                //TODO 聚合操作完成,把当前处理的时间更新一下
                        //从那张订单宽表中去查询
                   "from  ORDER_WIDE" +
//-------------------------------------------------按照这些字段维度进行分组,分完组开窗-------------------------------------------------
                        //开窗,通过group by开窗    rowtime:表示事件时间的属性字段    INTERVAL '10' SECOND:表示窗口大小
                    "group by  TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                        //分组的时候需要将这些维度加进去.
                    "province_id,province_name,province_area_code,province_iso_code,province_3166_2_code");

        //如果是Flink官方支持的数据库，也可以直接把目标数据表定义为动态表，用insert into 写入。
        //由于ClickHouse目前官方没有支持的jdbc连接器（目前支持Mysql、 PostgreSQL、Derby）。
        // 也可以制作自定义sink，实现官方不支持的连接器。但是比较繁琐。
        //TODO 4.将动态表转换为数据流   专门定义了javaBean来封装查询的结果ProvinceStats
        //toAppendStream:不涉及变化,直接insert 的可以用.    toRetractStream:涉及到变化的.最好直接用toRetractStream
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);
        //DataStream<Tuple2<Boolean, ProvinceStats>> provinceStatsDS = tableEnv.toRetractStream(provinceStateTable, ProvinceStats.class);

        //TODO 5.将流中的数据保存到ClickHouse
         provinceStatsDS.addSink(
            ClickHouseUtil.getJdbcSink(
                "insert into  province_stats  values(?,?,?,?,?,?,?,?,?,?)"
            )
        );
        env.execute();
    }
}
