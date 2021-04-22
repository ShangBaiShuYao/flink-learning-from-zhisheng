package com.shangbaishuyao.demo.FlinkDemo03;

import com.shangbaishuyao.demo.Function.MyEsSinkFunc;
import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.util.ArrayList;
/**
 * Author: shangbaishuyao
 * Date: 16:45 2021/4/22
 * Desc: 写入ES
 */
public class Flink09_Sink_Es {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
//        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]));
                    }
                });

        //3.将数据写入ES
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder =
                new ElasticsearchSink.Builder<WaterSensor>(httpHosts, new MyEsSinkFunc());
        //批量提交参数
        waterSensorBuilder.setBulkFlushMaxActions(1);
        ElasticsearchSink<WaterSensor> elasticsearchSink = waterSensorBuilder.build();
        waterSensorDS.addSink(elasticsearchSink);
        //4.执行任务
        env.execute();
    }
}
