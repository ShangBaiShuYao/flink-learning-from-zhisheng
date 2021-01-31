package com.shangbaishuyao.connectors.redis.utils;

import com.shangbaishuyao.common.model.ProductEvent;
import com.shangbaishuyao.common.utils.GsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Desc:
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 17:58 2021/1/31
 */
@Slf4j
public class ProductUtil {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "shangbaishuyao";  //kafka topic 需要和 flink 程序用同一个 topic

    public static final Random random = new Random();

    public static void main(String[] args) {
        Properties properties = new Properties();
        //Kafka Producer在发送消息时必须配置的参数为：bootstrap.servers、key.serializer、value.serializer。
        // 序列化操作是在拦截器（Interceptor）执行之后并且在分配分区(partitions)之前执行的。
        properties.put("bootstrap.servers", broker_list);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i=1; i<= 10000; i++){
            //lombok 下的@Builder注解用法 https://blog.csdn.net/qq_35568099/article/details/80438538
            ProductEvent productEvent = ProductEvent.builder().id((long) i)//商品id
                    .name("product" + i) //商品名称
                    //nextLong() 方法用于返回下一个伪均匀分布的随机数生成器的序列long值。
                    .price(random.nextLong() / 10000000000000L) //商品价格（以分为单位）
                    .code("code" + i).build();//商品编码

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, null, null, GsonUtil.toJson(productEvent));
            producer.send(producerRecord);
            System.out.println("发送数据: " + GsonUtil.toJson(productEvent));
        }
        producer.flush();
    }
}
