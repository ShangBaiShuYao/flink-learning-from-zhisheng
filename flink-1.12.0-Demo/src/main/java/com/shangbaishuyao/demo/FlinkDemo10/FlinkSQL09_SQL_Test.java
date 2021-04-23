package com.shangbaishuyao.demo.FlinkDemo10;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
/**
 * Author: shangbaishuyao
 * Date: 13:29 2021/4/23
 * Desc:
 */
public class FlinkSQL09_SQL_Test {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取端口数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });
        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS);
        //4.使用SQL查询未注册的表
        Table result = tableEnv.sqlQuery("select id,sum(vc) from " + table + " group by id");
        //5.将表对象转换为流进行打印输出
        tableEnv.toRetractStream(result, Row.class).print();
        //6.执行任务
        env.execute();
    }
}
