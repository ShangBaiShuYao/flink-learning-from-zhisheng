package com.shangbaishuyao.demo.FlinkDemo11;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.rowInterval;
/*
 * Author: shangbaishuyao
 * Date: 13:40 2021/4/23
 * Desc:
 */
public class FlinkSQL14_TableAPI_OverWindow_Bounded {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取端口数据创建流并转换每一行数据为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });
        //3.将流转换为表并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());
        //4.开启Over往前无界窗口
        Table result = table.window(Over
                .partitionBy($("id"))
                .orderBy($("pt"))
                .preceding(rowInterval(2L))
                .as("ow"))
                .select($("id"),
                        $("vc").sum().over($("ow")));

        //5.将结果表转换为流进行输出
        tableEnv.toAppendStream(result, Row.class).print();
        //6.执行任务
        env.execute();
    }
}
