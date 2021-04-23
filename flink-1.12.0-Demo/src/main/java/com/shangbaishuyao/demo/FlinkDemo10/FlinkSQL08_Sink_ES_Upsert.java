package com.shangbaishuyao.demo.FlinkDemo10;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;
/**
 * Author: shangbaishuyao
 * Date: 13:29 2021/4/23
 * Desc:
 */
public class FlinkSQL08_Sink_ES_Upsert {
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
                .groupBy($("id"), $("vc"))
                .select($("id"),
                        $("ts").count().as("ct"),
                        $("vc"));
        //6.将selectTable写入ES
        tableEnv.connect(new Elasticsearch()
                .index("sensor_sql")
                .documentType("_doc")
                .version("6")
                .host("hadoop102", 9200, "http")
                .keyDelimiter("-")
                .bulkFlushMaxActions(1))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ct", DataTypes.BIGINT())
                        .field("vc_sum", DataTypes.INT()))
                .withFormat(new Json())
                .inUpsertMode()
                .createTemporaryTable("sensor");
        selectTable.executeInsert("sensor"); //Sink
        //7.执行任务
        env.execute();
    }
}
