package com.shangbaishuyao.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;

/**
 * Desc: 操作Kafka的工具类 <br/>
 *
 * https://blog.csdn.net/u012164509/article/details/92836171
 * https://www.pianshen.com/article/27581061419/
 * kafka group_id解释:
 *
 * topic到group质检是发布订阅的通信方式，即一条topic会被所有的group消费，属于一对多模式；group到consumer是点对点通信方式，属于一对一模式。
 *
 * 不使用group的话，启动10个consumer消费一个topic，这10个consumer都能得到topic的所有数据，相当于这个topic中的任一条消息被消费10次。
 *
 * 使用group的话，连接时带上groupid，topic的消息会分发到10个consumer上，每条消息只被消费1次
 * 在一个消费者组当中可以有一个或者多个消费者实例，它们共享一个公共的group ID，组ID是一个字符串，用来唯一标志一个消费者组，
 * 组内的所有消费者协调在一起来消费订阅主题的所有分区，但是同一个topic下的某个分区只能被消费者组中的一个消费者消费，不同消费者组中的消费者可以消费相同的分区。
 *
 * 需要注意的是，如果消费者组当中消费者的数量超过了订阅主题分区的数量，那么多余的消费者就会被闲置，不会受到任何消息。
 * 同一个Topic对应很多的分区，这topic消息，可以被很多组消费者消费，但是同一个topic下的某个分区，只能被一个消费者组当中的一个消费者消费
 * 分区0，被消费者1消费，同时还被消费者2消费 --> 这个就是错的
 * 一个消费者组的一个消费者，可以消费一个topic下的多个分区
 * 同一个topic下的某个分区，可以被多个消费者组，消费者消息
 *
 * @Author: 上白书妖
 * @Date: 16:58 2021/4/9
 */
public class MyKafkaUtil2 {
    private static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static String DEFAULT_TOPIC = "DEFAULT_DATA";

    //TODO 获取FlinkKafkaConsumer 消费者消费数据 <br/>
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        //Kafka连接的一些属性配置
        Properties props = new Properties();
        //在一个消费者组当中可以有一个或者多个消费者实例，它们共享一个公共的group ID，组ID是一个字符串，
        // 用来唯一标志一个消费者组，组内的所有消费者协调在一起来消费订阅主题的所有分区，但是同一个topic
        // 下的某个分区只能被消费者组中的一个消费者消费，不同消费者组中的消费者可以消费相同的分区。
        //需要注意的是，如果消费者组当中消费者的数量超过了订阅主题分区的数量，那么多余的消费者就会被闲置，不会受到任何消息。
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

    //Flink SQL kafka DDL
    //拼接Kafka相关属性到DDL:创建表,修改表结构.
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
            " 'topic' = '"+topic+"',"   +
            " 'properties.bootstrap.servers' = '"+ KAFKA_SERVER +"', " +
            " 'properties.group.id' = '"+groupId+ "', " +
            " 'format' = 'json', " +
            " 'scan.startup.mode' = 'latest-offset'  ";
        return  ddl;
    }
}
