package com.shangbaishuyao.connectors.rabbitmq;

import com.shangbaishuyao.common.model.MetricEvent;
import com.shangbaishuyao.common.schemas.MetricSchema;
import com.shangbaishuyao.common.utils.ExecutionEnvUtil;
import com.shangbaishuyao.common.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * Desc: 从 kafka 读取数据 sink 到 rabbitmq <br/>
 * create by shangbaishuyao on 2021/2/1
 * @Author: 上白书妖
 * @Date: 13:39 2021/2/1
 */
public class Main1 {
    public static void main(String[] args) throws Exception{
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> dataSource = KafkaConfigUtil.buildSource(env);

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setVirtualHost("/")
                .setPort(5672)
                .setUserName("admin")
                .setPassword("admin")
                .build();


        //注意，换一个新的 queue，否则也会报错
        dataSource.addSink(new RMQSink<>(connectionConfig,"shangbaishuyao001",new MetricSchema()));
        env.execute("flink learning connectors rabbitmq");
    }
}
