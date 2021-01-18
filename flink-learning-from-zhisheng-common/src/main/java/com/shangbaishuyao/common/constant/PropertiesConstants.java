package com.shangbaishuyao.common.constant;
/**
 * Desc:配置常量<br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/13 11:04
 */
public class PropertiesConstants {
    public static final String SHANGBAISHUYAO = "shangbaishuyao";
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "shangbaishuyao"; //默认的kafka组id
    public static final String METRICS_TOPIC = "metrics.topic";
    public static final String CONSUMER_FROM_TIME = "consumer.from.time";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism"; //sink并行度
    public static final String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism"; //默认的并行度
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";//stream的checkPoint的开始
    public static final String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir"; //在HDFS中的文件夹地址
    public static final String STREAM_CHECKPOINT_TYPE = "stream.checkpoint.type";//Stream的CheckPoint的类型
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";//stream的checkPoint的间隔
    public static final String PROPERTIES_FILE_NAME = "/application.properties";
    public static final String CHECKPOINT_MEMORY = "memory";//checkPoint的内存
    public static final String CHECKPOINT_FS = "fs";
    public static final String CHECKPOINT_ROCKETSDB = "rocksdb"; //存储和访问数百PB的数据是一个非常大的挑战，开源的RocksDB就是FaceBook开放的一种嵌入式、持久化存储、KV型且非常适用于fast storage的存储引擎。: https://www.jianshu.com/p/3302be5542c7

    //ES的配置
    public static final String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "elasticsearch.bulk.flush.max.actions";
    public static final String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";

    //mysql的配置
    public static final String MYSQL_DATABASE = "mysql.database";
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_USERNAME = "mysql.username";


    //com.shangbaishuyao.sinks.sinks.MySinks.class 里面用到
    public static final String SINKS_KAFKAUTIL_TOPIC = "student"; //kafak topic 需要和flink 程序用同一个 topic

}
