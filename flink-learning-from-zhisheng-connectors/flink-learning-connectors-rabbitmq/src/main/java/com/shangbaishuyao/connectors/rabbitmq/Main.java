package com.shangbaishuyao.connectors.rabbitmq;

import com.shangbaishuyao.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * Desc: 从 rabbitmq 读取数据 <br/>
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 23:58 2021/1/31
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;

        //下面这些写死的参数可以放在配置文件中，然后通过 parameterTool 获取
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("127.0.0.1")
                .setVirtualHost("/")
                .setPort(5672)
                .setUserName("admin")
                .setPassword("admin").build();


        DataStreamSource<String> streamSource = env.addSource(new RMQSource<>(
                connectionConfig,
                "shangbaishuyao",
                true,
                new SimpleStringSchema())).setParallelism(1);

        streamSource.print();

        //如果想保证 exactly-once 或 at-least-once 需要把 checkpoint 开启
//        env.enableCheckpointing(10000);
        env.execute("flink learning connectors rabbitmq");
    }
}
