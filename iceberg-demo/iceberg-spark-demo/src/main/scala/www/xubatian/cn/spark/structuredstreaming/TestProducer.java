package www.xubatian.cn.spark.structuredstreaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class TestProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092");
        props.put("acks", "-1");
        props.put("batch.size", "1048576");
        props.put("linger.ms", "5");
        props.put("compression.type", "snappy");
        props.put("buffer.memory", "33554432");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        Random random = new Random();
        for (int i = 0; i < 10000000; i++) {
            producer.send(new ProducerRecord<String,String>("test2",i+"\t"+random.nextInt(100)+"\t"+random.nextInt(3)+"\t"+System.currentTimeMillis()));
        }
        producer.flush();
        producer.close();
    }
}
