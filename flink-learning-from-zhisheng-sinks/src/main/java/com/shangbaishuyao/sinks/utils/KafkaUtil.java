package com.shangbaishuyao.sinks.utils;

import com.shangbaishuyao.common.utils.GsonUtil;
import com.shangbaishuyao.sinks.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static com.shangbaishuyao.common.constant.PropertiesConstants.DEFAULT_KAFKA_BROKERS;
import static com.shangbaishuyao.common.constant.PropertiesConstants.SINKS_KAFKAUTIL_TOPIC;

/**
 * Desc: 生产者kafka(kafkaProducer)往kafka中写数据,可以使用这个main函数进行测试 <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/24 16:39
 */
public class KafkaUtil {
//    public static final String broker_list = "localhost:9092";
//    public static final String topic = "student"; //kafak topic 需要和flink 程序用同一个 topic

    /**
     * 如果一个线程被interrupt，并在之后进行sleep，wait，join这三种操作，那么InterruptedException就会被触发  https://www.jianshu.com/p/87050b0b835b
     * @throws InterruptedException
     */
    public static void writeToKafka() throws InterruptedException{
        Properties props = new Properties();
        props.put("bootstrap.servers",DEFAULT_KAFKA_BROKERS);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //初始化kafka的生产者对象,将kafka配置设置到kafka的生产者对象里面
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //for循环100条,异步的将记录发送到kafka主题
        for (int i = 1; i <= 100 ; i++){
            Student student = new Student(i, "shangbaishuyao" + i, "password" + i, 15 + i);
                /**
                 * Creates a record to be sent to a specified topic and partition
                 * 创建要发送到指定主题和分区的记录
                 *
                 * @param topic 记录将附加到的主题           (The topic the record will be appended to)
                 * @param partition 记录应该被发送到的分区    (The partition to which the record should be sent)
                 * @param key 将包含在记录中的键             (The key that will be included in the record)
                 * @param value 记录内容                   (The record contents)
                 *
                 *  public ProducerRecord(String topic, Integer partition, K key, V value) {
                 *    this(topic, partition, null, key, value, null);
                 *   }
                 *
                 *
                 * 单词: record 记录
                 **/
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(SINKS_KAFKAUTIL_TOPIC, null, null, GsonUtil.toJson(student));

                /**
                 * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
                 * 异步地将记录发送到主题。相当于send(record, null)。
                 *
                 * See {@link #send(ProducerRecord, Callback)} for details.
                 * 详情请参见{@link #send(ProducerRecord, Callback)}
                 *
                 *  @Override
                 *  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
                 *      return send(record, null);
                 *  }
                 **/
            producer.send(record);
            System.out.println("发送数据: " + GsonUtil.toJson(student));
        }
        //刷新生成器中的累积记录
        producer.flush();
    }

    //测试writeToKafka()方法
    public static void main(String[] args) throws InterruptedException{
        writeToKafka();
    }
}
