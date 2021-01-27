package com.shangbaishuyaoconnectors.rocketmq.example;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/*
 * Desc: 一个简单的生产者生产消息的例子  <br/>
 * create by shangbaishuyao on 2021/1/28
 * @Author: 上白书妖
 * @Date: 0:16 2021/1/28
 */
public class SimpleProducer {
    public static void main(String[] args) {
        //①设置生产者基本配置信息,即生产消息到哪里
        DefaultMQProducer producer = new DefaultMQProducer("p001");
        producer.setNamesrvAddr("localhost:9876");

        //②开启生产者
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }


        //③遍历循环生产10000条消息
        for (int i = 0; i < 10000; i++) {
            Message msg = new Message("shangbaishuyao", "", "id_" + i, ("country_X province_" + i).getBytes());

            //④生产发送生产完成后的消息
            try {
                producer.send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }

            //打印消息条数
            System.out.println("send " + i);
            try {
                //加延迟
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
