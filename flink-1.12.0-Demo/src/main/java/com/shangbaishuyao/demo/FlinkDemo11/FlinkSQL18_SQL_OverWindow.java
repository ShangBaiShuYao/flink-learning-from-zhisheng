package com.shangbaishuyao.demo.FlinkDemo11;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
/*
 * Author: shangbaishuyao
 * Date: 13:41 2021/4/23
 * Desc:
 */
public class FlinkSQL18_SQL_OverWindow {
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
        //4.SQL API实现滑动时间窗口
        Table result = tableEnv.sqlQuery("select " +
                "id," +
                "sum(vc) over w as sum_vc," +
                "count(id) over w as ct " +
                "from " + table +
                " window w as (partition by id order by pt rows between 2 preceding and current row)");

        //报错,FlinkSQL中只能有一种Over窗口
//        Table result = tableEnv.sqlQuery("select " +
//                "id," +
//                "sum(vc) over (partition by id order by pt) as sum_vc," +
//                "count(id) over (order by pt) as ct " +
//                "from " + table);

        //5.将结果表转换为流进行输出
//        tableEnv.toAppendStream(result, Row.class).print();
        //直接输出表
        result.execute().print();
        //6.执行任务
        env.execute();
    }
}
