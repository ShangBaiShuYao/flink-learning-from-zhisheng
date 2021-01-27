package com.shangbaishuyaoconnectors.rocketmq.example;

import com.shangbaishuyaoconnectors.rocketmq.RocketMQConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Desc: 从 RocketMQ 中获取数据后写入到 RocketMQ <br/>
 * created by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 16:30 2021/1/19
 */
public class RocketMQToRocketMQFlinkExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000); //启动checkPoint并设置间隔

        //初始化消费者配置
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(RocketMQConfig.NAME_SERVER_ADDR,"localhost:9876");
        int msgDelayLevel05 = RocketMQConfig.MSG_DELAY_LEVEL05;
        consumerProperties.setProperty(RocketMQConfig.MSG_DELAY_LEVEL, String.valueOf(msgDelayLevel05));
        //TimeDelayLevel is not supported for batching 批量处理不支持TimeDelayLevel
        boolean batcheFlag = msgDelayLevel05 <= 0;
        
//        env.addSource()
    }
}
