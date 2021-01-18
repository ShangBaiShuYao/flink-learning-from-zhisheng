package com.shangbaishuyao.sources;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Desc: 利用kafka 自带的 source 读取 kafka 里面的数据 <br/>
 * create by shangbaishuyao 2020-12-11
 *@Author: 上白书妖
 *@Date: 2020/12/11 15:14
 */
public class Main {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //value 反序列化
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>(
                                                 "metric",  //kafka topic
                                                       new SimpleStringSchema(), //String 序列化
                                                       props)).setParallelism(1);

        dataStreamSource.print();//将从kafka读取到的数据打印到控制台

        env.execute("Flink add data source");
    }
}
