package com.shangbaishuyao.common.schemas;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 *Desc: kafka度量主题<br/>
 *Kafka MetricSchema ，支持序列化和反序列化<br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/13 11:55
 */
public class KafkaMetricSchema implements KafkaDeserializationSchema<ObjectNode> , Serializable {


    //单词:includeMetadata 包含元数据
    private final boolean includeMetadata;

    private ObjectMapper mapper;

    /**
     * ==================实现重写方法===================<br/>
     */

    @Override
    public boolean isEndOfStream(ObjectNode nextElement) {
        return false;
    }

    //自定义反序列化类从kafka中消费键值对的消息          单词:ConsumerRecord 消费记录
    @Override
    public ObjectNode deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        if (consumerRecord == null){
            mapper = new ObjectMapper();
        }

        /**
         * String consumerRecordValue = new String(consumerRecord.value());
         *
         * 以这种方式赋值时，JVM会先从字符串实例池中查询是否存在"consumerRecord.value()"这个对象，
         * 若不存在则会在实例池中创建"consumerRecord.value()"对象，同时在堆中创建"consumerRecord.value()"这个对象，然后将堆中的这个对象的地址返回赋给引用consumerRecordValue。
         * 若实例池存在则直接在堆中创建"consumerRecord.value()"这个对象，然后将堆中的这个对象的地址返回赋给引用consumerRecordValue。
         */
        ObjectNode node = mapper.createObjectNode();
        //判断从kafka里面获取的键值对是否不为空
        if (consumerRecord.key() != null){
            node.put("key", new String(consumerRecord.key()));
        }

        if (consumerRecord.value() != null){
            node.put("value", new String(consumerRecord.value()));
        }

        //获取我们需要的字段数据
        if (includeMetadata){
            node.putObject("matedata")
                    .put("offset", consumerRecord.offset())
                    .put("topic", consumerRecord.topic())
                    .put("partition", consumerRecord.partition());
        }
        return node;
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return getForClass(ObjectNode.class);
    }




    /**
     * =================Get,Set,构造方法================<br/>
     */
    public boolean isIncludeMetadata() {
        return includeMetadata;
    }

    public KafkaMetricSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

}
/**
 *
 * Flink消费Kafka:自定义KafkaDeserializationSchema<br/>
 * kafka中的数据通常是键值对的，所以我们这里自定义反序列化类从kafka中消费键值对的消息<br/>
 *
 *
 * Desc: kafkaMeticSchema 实现了kafkaDeserializtionShema,而kafkaDeserializtionShema继承了反序列化类.可以获取的元数据<br/>
 * 注意: kafkaDeserializtionShema和JSONkeyValueDeserializtionSchema有个却别,就是kafkaDeserializtionShema不需要数据源的数据是json<br/>
 *
 *
 * 介绍关于ObjectNode博客: https://blog.csdn.net/neweastsun/article/details/100669516  是不是暖暖的很贴心^_^?
 *
 *
 * 对于ObjectMapper类的介绍博客: https://blog.csdn.net/qq_32454537/article/details/80672191
 *  ObjectMapper类是Jackson库的主要类。它提供一些功能将转换成Java对象匹配JSON结构，反之亦然。它使用JsonParser和JsonGenerator的实例实现JSON实际的读/写。
 *
 * */