package com.shangbaishuyao.demo.FlinkDemo12;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
/*
 * Author: shangbaishuyao
 * Date: 13:45 2021/4/23
 * Desc: 自定义UDTF函数
 */
public class FlinkSQL04_Function_UDTF {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS);
        //4.先注册再使用
        tableEnv.createTemporarySystemFunction("split", Spilt.class);
        //TableAPI
//        table
//                .joinLateral(call("split", $("id")))
//                .select($("id"), $("word"))
//                .execute()
//                .print();
        //SQL
        tableEnv.sqlQuery("select id, word from "+ table + ",lateral table(split(id))").execute().print();
        //6.执行任务
        env.execute();
    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class Spilt extends TableFunction<Row> {
        public void eval(String value) {
            String[] split = value.split("_");
            for (String s : split) {
                collect(Row.of(s));
            }
        }
    }
}
