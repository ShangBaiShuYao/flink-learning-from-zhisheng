package com.shangbaishuyao.demo.FlinkDemo10;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;
/*
 * 表到流的转换:
 * Append-only 流（追加流）
 * Retract 流（撤回流，使用聚合操作，count，sum等）
 * Upsert 流(更新流,直接更新)
 *
 * 注意: 在将动态表转换为 DataStream 时，只支持 append 流和 retract 流。
 * 只有当我们对接Hbase,ES等这些外部系统的时候才会有upsert模式.
 * Author: shangbaishuyao
 * Date: 23:00 2021/4/23
 * Desc:
 */
public class FlinkSQL06_Sink_Kafka {
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
        //5.使用TableAPI过滤出"ws_001"的数据
        Table selectTable = sensorTable
                .where($("id").isEqual("ws_001"))
                .select($("id"), $("ts"), $("vc"));
        //6.将selectTable写入Kafka
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("test")
                .startFromLatest()
                .sinkPartitionerRoundRobin()
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.INT()))
//                .withFormat(new Json())
                .withFormat(new Csv())
                .createTemporaryTable("sensor");
//        tableEnv.from("sensor");             //Source
        selectTable.executeInsert("sensor"); //Sink
        //7.执行任务
        env.execute();
    }
}
