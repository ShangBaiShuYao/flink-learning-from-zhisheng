package com.shangbaishuyaoconnectors.rocketmq.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/*
 * Desc: 一个简单的消费者消费消息的例子  <br/>
 *
 * create by shangbaishuyao on 2021/1/27
 * @Author: 上白书妖
 * @Date: 23:59 2021/1/27
 */
public class SimpleConsumer {
    public static void main(String[] args) {
        //① 指定消费者消费的来源/地址,指定消费者组
        //消费者默认来去消息,从那个消费组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("g00003");
        consumer.setNamesrvAddr("localhost:9876");

        //② 指定消费者订阅的主题,订阅的范围
        try {
            //消费者订阅的主题 ,  if null or * expression,meaning subscribe all(如果为null或*表达式，表示订阅所有)
            consumer.subscribe("shangbaishuyao", "*");
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        //③监听消费者是否成功消费,返回消费成功的状态
        //注册一个回调，以便在消息到达时执行并发消费
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(msg.getKeys() + ":" + new String(msg.getBody()));
                }
                //返回消费者消费成功消费同时发生的状态
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


        //④开启消费者消费消息
        try {
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
