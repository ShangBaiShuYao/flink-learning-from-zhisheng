package com.shangbaishuyao.sources.utils;

import com.shangbaishuyao.common.constant.PropertiesConstants;
import com.shangbaishuyao.common.model.MetricEvent;
import com.shangbaishuyao.common.utils.GsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Desc: 往kafka里面写数据,可以使用此main函数进行测试 <br/>
 * create by shangbaishuyao 2020-12-11
 *@Author: 上白书妖
 *@Date: 2020/12/11 14:26
 */
public class KafkaUtil {
    private static final String broker_list = PropertiesConstants.DEFAULT_KAFKA_BROKERS;
    private static final String topic = "metric"; //kafka topic, Flin程序中需要和这个统一

    private static void writeToKafk() throws InterruptedException{
        Properties props = new Properties();
        props.put("bootstrap.servers",broker_list);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");//key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//value序列化

        //设置生产者配置
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        //初始化度量事件实体类,等下用来装数据
        MetricEvent metric = new MetricEvent();
        metric.setTimestamp(System.currentTimeMillis()); //System.currentTimeMillis() 系统时间，也就是日期时间，可以被系统设置修改，然后值就会发生跳变。
        metric.setName("mem");
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();

        tags.put("cluster", "hadoop102");
        tags.put("host_ip","192.168.6.102");


        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags);
        metric.setFields(fields);

        //创建要发送到指定主题和分区的记录
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, null, GsonUtil.toJson(metric));
        kafkaProducer.send(record);
        System.out.println("发送数据" + GsonUtil.toJson(metric));

        kafkaProducer.flush();
    }

    public static void main(String[] args) throws InterruptedException{
        while (true){
            Thread.sleep(300);
            writeToKafk();
        }
    }
}
