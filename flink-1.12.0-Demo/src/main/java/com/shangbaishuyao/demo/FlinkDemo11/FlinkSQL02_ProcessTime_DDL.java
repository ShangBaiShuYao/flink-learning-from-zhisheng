package com.shangbaishuyao.demo.FlinkDemo11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/**
 * Author: shangbaishuyao
 * Date: 13:32 2021/4/23
 * Desc:
 */
public class FlinkSQL02_ProcessTime_DDL {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用DDL的方式指定处理时间字段
        tableEnv.executeSql("create table source_sensor (id string, ts bigint, vc int,pt as PROCTIME()) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_source',"
                + "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                + "'properties.group.id' = 'shangbaishuyao',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'csv'"
                + ")");
        //3.将DDL创建的表生产动态表
        Table table = tableEnv.from("source_sensor");
        //4.打印元数据信息
        table.printSchema();
    }
}
