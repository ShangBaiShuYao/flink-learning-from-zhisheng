package com.shangbaishuyao.connectors.mysql.KafkaUtil;

import com.shangbaishuyao.common.utils.GsonUtil;
import com.shangbaishuyao.connectors.mysql.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/*
 * Desc: 往kafka中写数据,可以使用这个main函数进行测试 <br/>
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 14:52 2021/1/31
 */
public class KafkaUtil {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "student";  //kafka topic 需要和 flink 程序用同一个 topic

    //写入kafka
    public static void writeToKafka() throws InterruptedException{
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        //初始化生产者
        KafkaProducer producer = new KafkaProducer<>(props);


        //生产者生产数据
        for (int i = 1 ; i<=100 ;i++ ){
            Student student = new Student(i, "shangbaishuyao" + i, "password" + i, 18 + i);
            //初始化生产者记录对象
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, null, GsonUtil.toJson(student));
            producer.send(record);
            System.out.println("发送数据:" + GsonUtil.toJson(student));
            //发送一条数据 sleep(沉睡) 10s，相当于 1 分钟 6 条
            Thread.sleep(10*1000);
        }
        //刷新生产程序中的累计记录
        producer.flush();
    }
    
    
    //主函数
    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}
