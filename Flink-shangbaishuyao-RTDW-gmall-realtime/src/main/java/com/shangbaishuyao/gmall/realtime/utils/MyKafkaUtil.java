package com.shangbaishuyao.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;

/**
 * Desc: 操作Kafka的工具类 <br/>
 * @Author: 上白书妖
 * @Date: 16:58 2021/4/9
 */
public class MyKafkaUtil {
    private static String KAFKA_SERVER = "hadoop202:9092,hadoop203:9092,hadoop204:9092";
    private static String DEFAULT_TOPIC = "DEFAULT_DATA";

    //TODO 获取FlinkKafkaConsumer 消费者消费数据 <br/>
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        //Kafka连接的一些属性配置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }

    //TODO 1.封装FlinkKafkaProducer 生产者生产数据 ,只能发送到指定的一个主题<br/>
    //TODO 只能序列化字符串对象
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(KAFKA_SERVER, topic, new SimpleStringSchema());
    }

    //TODO 2.封装FlinkKafkaProducer 生产者生产数据,可以指定发送到不同的主题 <br/>
    //TODO 自定义kafka的序列化,由我来指定序列化什么类型的对象
    //new  SimpleStringSchema(): 这个是做序列化的,只能序列化String类型. 如果序列化JSON的话,他不能序列化.
    //如果想把JSON保存到kafka里面去,就得自己去重写这个kafka的序列化.
    //不管是json,图片,字符串,对象.他底层都是字节数组
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        //kafka基本配置
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        //设置生产数据的超时时间.有时候生产者生产数据的时候会超时. 15*60*1000+"" --->这是装转换为字符串 15分钟*60秒*1000毫秒
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    //拼接Kafka相关属性到DDL:创建表,修改表结构.
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
            " 'topic' = '"+topic+"',"   +
            " 'properties.bootstrap.servers' = '"+ KAFKA_SERVER +"', " +
            " 'properties.group.id' = '"+groupId+ "', " +
            "  'format' = 'json', " +
            "  'scan.startup.mode' = 'latest-offset'  ";
        return  ddl;
    }
}
