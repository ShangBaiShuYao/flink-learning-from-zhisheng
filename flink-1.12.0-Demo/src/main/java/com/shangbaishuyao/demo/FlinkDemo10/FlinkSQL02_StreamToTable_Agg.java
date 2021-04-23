package com.shangbaishuyao.demo.FlinkDemo10;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
/**
 * 表到流的转换:
 * Append-only 流（追加流）
 * Retract 流（撤回流，使用聚合操作，count，sum等）
 * Upsert 流(更新流,直接更新)
 *
 * 注意: 在将动态表转换为 DataStream 时，只支持 append 流和 retract 流。
 * 只有当我们对接Hbase,ES等这些外部系统的时候才会有upsert模式.
 *
 * Author: shangbaishuyao
 * Date: 13:28 2021/4/23
 * Desc: 聚合操作, 求总和,某个count或者sum
 * 注意: 聚合操作需要使用撤回流,不能使用追加流
 */
public class FlinkSQL02_StreamToTable_Agg {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据创建流并装换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });
        //3.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //4.将流转换为动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);
        //5.使用TableAPI 实现 : select id,sum(vc) from sensor where vc>=20 group by id;
        //TODO Flink V1.12.0新版本的写法
//        Table selectTable = sensorTable
//                .where($("vc").isGreaterOrEqual(20)) //水位线大于等于20的
//                .groupBy($("id"))                    //做聚合操作肯定是要分组
//                .aggregate($("vc").sum().as("sum_vc")) //聚合vc 按照sum聚合,区别名sum_vc
//                .select($("id"), $("sum_vc"));       //查询数据, id可以查,但是ts不能查询,因为我并没有按照ts进行聚合
        //TODO 老版本的写法
        Table selectTable = sensorTable
                .groupBy("id")
                .select("id,id.sum");
        //6.将selectTable转换为流进行输出
        // toAppendStream: 追加. 来一条数据往动态表里面去追加,来一条追加一条. 但是涉及到一个问题, 第一条数据20则输出20,第二条数据20,应该输出40,但是以前的20怎么办呢?
        // 会报错. toAppendStream 追加流. 聚合操作本来是20, 但是追加后是40, 这个40能聚合进去吗?不能,不然我同一个id出现了多次.
        // 所以我们使用toRetractStream 撤回流. 这个Boolean是干什么用的呢? 是问你要不要撤回.
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(selectTable, Row.class);
        rowDataStream.print();
        //7.执行任务
        env.execute();
    }
}
