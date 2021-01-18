package com.shangbaishuyao.common.utils;

import com.shangbaishuyao.common.model.MetricEvent;
import com.shangbaishuyao.common.schemas.MetricSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.shangbaishuyao.common.constant.PropertiesConstants.*;

/**
 * Desc: kafka 配置工具类 <br/>
 *
 * 知识补充: <br/>
 *      ①ParameterTool简介          https://www.jianshu.com/p/a71b0ed7ef15      <br/>
 *      ②DataStreamSource简介:      https://www.jianshu.com/p/521b7268fd55      <br/>
 *      ③FlinkKafkaProducer011简介:
 *      ④Properties简述:            https://www.jianshu.com/p/29e0983dbaa8      <br/>
 *      ⑤优雅的使用Kafka Consumer:   https://www.jianshu.com/p/abbc09ed6703      <br/>
 *
 * Flink官方:  <br/>
 *      ①flink整合kafka官网介绍:     https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/connectors/kafka.html <br/>
 *
 * 个人理解笔记: <br/>
 *     一. KafkaConfigUtil中有kafka的基础设置,但是这个kafka基础设置是设置在ParameterTool工具类中的 <br/>
 *     二. 通过订阅时间构建分区偏移量,设置通过分区(partition)偏移量设置具体的偏移量(offset) ,最后构建Flink读取Kafka中数据的数据源(source) <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/19 15:35
 */
public class KafkaConfigUtil {


    /**
     * Desc: ①设置基础的 kafka 配置 <br/>
     *
     * ①调用②设置基础配置 <br/>
     *
     * @return kafkaBasicProps 基础kafka配置
     */
    public static Properties buildKafkaBasicProps(){
        Properties kafkaBasicProps = buildKafkaProps(ParameterTool.fromSystemProperties());
        return kafkaBasicProps;
    }


    /**
     * Desc: ②设置 kafka 配置 <br/>
     *
     * @param  parameterTool
     * @return properties
     */
    public static Properties buildKafkaProps(ParameterTool parameterTool){
        Properties properties = parameterTool.getProperties();
        properties.put("bootstrap.servers", parameterTool.get(KAFKA_BROKERS,DEFAULT_KAFKA_BROKERS));
        properties.put("zookeeper.connect",parameterTool.get(KAFKA_ZOOKEEPER_CONNECT,DEFAULT_KAFKA_ZOOKEEPER_CONNECT));
        properties.put("group.id",parameterTool.get(KAFKA_GROUP_ID,DEFAULT_KAFKA_GROUP_ID));
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");
        return properties;
    }


    /**
     * Desc: ①构建Flink读取Kafka中数据的数据源(source) <br/>
     *
     * ①调用②,②调用③
     *
     * @param streamExecutionEnvironment Flink流式执行环境
     * @return flinkGainkafkaSource Flink获取Kafka消费者端的数据源
     * @throws Exception
     */
    //DataStreamSource简介: https://www.jianshu.com/p/521b7268fd55
    public static DataStreamSource<MetricEvent> buildSource(StreamExecutionEnvironment streamExecutionEnvironment) throws Exception{
        //使用env.getConfig().setGlobalJobParameters将ParameterTool的访问范围设置为global
        ParameterTool parameterTool = (ParameterTool) streamExecutionEnvironment.getConfig().getGlobalJobParameters(); //执行环境得到执行环境的配置里面配置ParameterTool的访问范围
        String topic = parameterTool.getRequired(METRICS_TOPIC);
        //订阅时间
        Long time = parameterTool.getLong(CONSUMER_FROM_TIME, 0L);
        //将定义好的消费者配置设置到执行环境中充当消费源,这样一旦启动Flink程序就可以根据我们的定义流式获取kafka中的数据
        DataStreamSource<MetricEvent> flinkGainkafkaSource = buildSourceSpecificOffsets(streamExecutionEnvironment, topic, time);
        //flinkGainkafkaSource: Flink获取Kafka消费者端的数据源
        return flinkGainkafkaSource;
    }


    /**
     * Desc: ②设置通过分区(partition)偏移量设置具体的偏移量(offset) <br/>
     *
     * @param streamExecutionEnvironment 执行环境
     * @param topic kafka主题
     * @param time 订阅时间
     * @return setStartFromSpecificOffsets 通过分区偏移(offset)设置具体的(specific)偏移量(offset)
     * @throws IllegalAccessException 表示没有访问权限的异常,因为我下面一个方法用的是private修饰的
     */
    public static DataStreamSource<MetricEvent> buildSourceSpecificOffsets(StreamExecutionEnvironment streamExecutionEnvironment,String topic,Long time) throws IllegalAccessException{
          //使用streamExecutionEnvironment.getConfig().setGlobalJobParameters将ParameterTool的访问范围设置为global
          ParameterTool parameterTool = (ParameterTool) streamExecutionEnvironment.getConfig().getGlobalJobParameters();
          Properties props = buildKafkaProps(parameterTool);
        /**
         * 构造方法入参
         * topicId：push的目标kafka topic
         * brokerList：kafka broker列表
         * SerializationSchema类：序列化方式
         * Properties类：可以包含kafka和zookeeper地址 以及消费组id
         */
        //自定义Flink-kafka的消费者source
        FlinkKafkaConsumer011<MetricEvent> consumer011 = new FlinkKafkaConsumer011<>(
                topic,
                new MetricSchema(),
                props);

        //重置offset到time时刻
        if (time != 0L){
            //得到分区偏移量
            Map<KafkaTopicPartition, Long> partitionOffset = buildOffsetByTime(props, parameterTool, time);
            consumer011.setStartFromSpecificOffsets(partitionOffset);
        }
        DataStreamSource<MetricEvent> setStartFromSpecificOffsets = streamExecutionEnvironment.addSource(consumer011);
        return  setStartFromSpecificOffsets;
    }

    /**
     * Desc: ③通过订阅时间构建分区偏移量 <br/>
     *
     * @param props
     * @param parameterTool
     * @param time  订阅的时间
     * @return partitionOffset 分区偏移量
     */
    private static Map<KafkaTopicPartition,Long> buildOffsetByTime(Properties props, ParameterTool parameterTool,Long time){
        //java.util.Properties.setProperty(String key,String value) 方法调用Hashtable的方法put。提供并行的getProperty方法。强制使用字符串的属性键和值。返回值是Hashtable调用put的结果。
        props.setProperty("group.id","query_time" + time);
        /**
         * KafkaConsumer():
         * 消费者通过提供{@link java.util.Properties} 来实例化对象作为配置。      单词:link 友情链接
         * 有效的配置字符串记录在{@link ConsumerConfig}
         * @param 用户配置属性
         */
        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        //org.apache.kafka.common.PartitionInfo;
        //通过topic指定的key获取kafka中的value值,也就是消费数据;key我们是在类PropertiesConstants中定义好了
        List<PartitionInfo> partitionsFor = kafkaConsumer.partitionsFor(parameterTool.getRequired(METRICS_TOPIC));
        //初始化
        HashMap<TopicPartition, Long> topicPartitionTimeLongMap = new HashMap<>();
        //遍历得到的数据,将每条数据的topic,time,partition都设置到我们的partitionInfoLongMap中
        for (PartitionInfo partitionInfo:partitionsFor){
            //查询在哪个主题里
            String topic = partitionInfo.topic();
            //查看在哪个分区里面
            int partition = partitionInfo.partition();
            //订阅时间       单词:subscription 订阅
            Long subScriptionTime = time;

            //主题-分区对象都设置进去
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            /**
             * put():
             * 在此映射中将指定的值与指定的键关联。
             * 如果以前的映射包含键的映射，则旧的
             * 值被替换
             *
             * 这里将主题-分区对象作为key ,将查询时间作为value
             */
            topicPartitionTimeLongMap.put(topicPartition,subScriptionTime);
        }
        /**
         * 按时间戳查找给定分区的偏移量。
         * 每个分区的返回偏移量是其时间戳大于或等于相应分区中给定时间戳的最早偏移量。
         * 这是一个阻塞调用。不需要为使用者分配分区。
         * 如果一个分区中的消息格式版本在0.10.0之前，即消息没有时间戳，那么该分区将返回null。
         * 注意，如果分区不存在，此方法可能会无限期阻塞。
         */
        //TopicPartition: 主题名称和分区号的类 (A topic name and partition number)
        //OffsetAndTimestamp: 用于偏移量和时间戳的容器类 (A container class for offset and timestamp.)      单词:offset 偏移量
        Map<TopicPartition, OffsetAndTimestamp> offsetResult = kafkaConsumer.offsetsForTimes(topicPartitionTimeLongMap);
        //初始化分区偏移量的对象
        HashMap<KafkaTopicPartition, Long> partitionOffset = new HashMap<>();

        //forEach遍历有数据的map集合,使用有数据的参数(key,value)键值对去用初始化的hashMap集合接收他的参数
        offsetResult.forEach( (key,value)   ->   partitionOffset.put(new KafkaTopicPartition( key.topic(),key.partition())   ,  value.offset()  ));

        /**
         * 关闭使用者，等待30秒的默认超时来进行所需的清理。如果启用了自动提交，它将在默认超时内尽可能提交当前偏移量。
         * 详情请参阅close(long, TimeUnit)。注意，wakeup()不能用于中断close
         */
        kafkaConsumer.close();
        //返回分区偏移量
        return  partitionOffset;
    }
}
