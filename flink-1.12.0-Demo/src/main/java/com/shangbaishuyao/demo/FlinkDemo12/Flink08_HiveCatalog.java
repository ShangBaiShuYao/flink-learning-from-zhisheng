package com.shangbaishuyao.demo.FlinkDemo12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
/*
 * Author: shangbaishuyao
 * Date: 13:43 2021/4/23
 * Desc:
 */
public class Flink08_HiveCatalog {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog("myHive", "default", "input");
        //3.注册HiveCatalog
        tableEnv.registerCatalog("myHive", hiveCatalog);
        //4.使用HiveCatalog
        tableEnv.useCatalog("myHive");
        //5.执行查询,查询Hive中已经存在的表数据
        tableEnv.executeSql("select * from business2").print();
    }
}

