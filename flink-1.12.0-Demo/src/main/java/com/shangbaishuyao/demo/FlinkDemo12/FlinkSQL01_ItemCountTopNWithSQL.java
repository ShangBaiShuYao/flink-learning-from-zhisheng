package com.shangbaishuyao.demo.FlinkDemo12;

import com.shangbaishuyao.demo.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
/*
 * Author: shangbaishuyao
 * Date: 13:43 2021/4/23
 * Desc: 使用Flink SQL 取TopN数据
 */
public class FlinkSQL01_ItemCountTopNWithSQL {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,build);

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

        //4.将userBehaviorDS流转换为动态表并指定事件时间字段
        Table table = tableEnv.fromDataStream(userBehaviorDS,
                $("userId"),
                $("itemId"),
                $("categoryId"),
                $("behavior"),
                $("timestamp"),
                $("rt").rowtime());

        //5.使用 Flink SQL实现滑动窗口内部计算每个商品被点击的总数
        Table windowItemCountTable = tableEnv.sqlQuery("select " +
                " itemId," +
                " count(itemId) as ct," +
                " HOP_END(rt, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd " +
                " from " + table +
                " group by itemId,HOP(rt, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)");

        //6.按照窗口关闭时间分组,排序
        Table rankTable = tableEnv.sqlQuery("select " +
                " itemId," +
                " ct," +
                " windowEnd," +
                " row_number() over(partition by windowEnd order by ct desc) as rk" +
                " from " + windowItemCountTable);

        //7.取TopN
        Table resultTable = tableEnv.sqlQuery("select * from " + rankTable + " where rk<=5");

        //8.打印结果
        resultTable.execute().print();

        //9.执行任务
        env.execute();
    }
}
