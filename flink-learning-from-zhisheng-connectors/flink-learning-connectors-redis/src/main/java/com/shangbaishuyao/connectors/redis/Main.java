package com.shangbaishuyao.connectors.redis;

import com.shangbaishuyao.common.constant.PropertiesConstants;
import com.shangbaishuyao.common.model.ProductEvent;
import com.shangbaishuyao.common.utils.ExecutionEnvUtil;
import com.shangbaishuyao.common.utils.GsonUtil;
import com.shangbaishuyao.common.utils.KafkaConfigUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/*
 * Desc: 从 Kafka 中读取数据然后写入到 Redis <br/>
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 17:57 2021/1/31
 */
public class Main {
    public static void main(String[] args) throws Exception{

        final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);


        //添加数据源
        SingleOutputStreamOperator<Tuple2<String, String>> product = env.addSource(new FlinkKafkaConsumer011<>(
                parameterTool.get(PropertiesConstants.METRICS_TOPIC),//这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).map(string -> GsonUtil.fromJson(string, ProductEvent.class))
                .flatMap(new FlatMapFunction<ProductEvent, Tuple2<String, String>>() {
                    @Override                                 //收集
                    public void flatMap(ProductEvent value, Collector<Tuple2<String, String>> out) throws Exception {
                        //收集商品 id 和 price 两个属性
                        out.collect(new Tuple2<>(value.getId().toString(), value.getPrice().toString()));
                    }
                });

        //单个redis
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost(parameterTool.get("redis.host")).build();

        product.addSink(new RedisSink<Tuple2<String, String>>(config,new RedisSinkMapper()));

        //Redis 的 ip 信息一般都从配置文件取出来
        //Redis 集群
/*        FlinkJedisClusterConfig clusterConfig = new FlinkJedisClusterConfig.Builder()
                .setNodes(new HashSet<InetSocketAddress>(
                        Arrays.asList(new InetSocketAddress("redis1", 6379)))).build();*/

        //Redis Sentinels
/*        FlinkJedisSentinelConfig sentinelConfig = new FlinkJedisSentinelConfig.Builder()
                .setMasterName("master")
                .setSentinels(new HashSet<>(Arrays.asList("sentinel1", "sentinel2")))
                .setPassword("")
                .setDatabase(1).build();*/

        env.execute("flink redis connector");
    }





    public static class RedisSinkMapper implements RedisMapper<Tuple2<String,String>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            RedisCommandDescription redisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "shangbaishuyao");
            return redisCommandDescription;
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
