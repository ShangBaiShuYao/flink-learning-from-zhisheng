package com.shangbaishuyao.connectors.rabbitmq.utils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Desc: 生产消息 <br/>
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 22:06 2021/1/31
 */
public class RabbitMQProducerUtil {
    public final static String QUEUE_NAME = "shangbaishuyao";

    public static void main(String[] args) throws Exception{

        //创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();

        //设置RibbitMQ相关信息
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connectionFactory.setPort(5672);

        //创建一个新的连接
        Connection connection = connectionFactory.newConnection();
        
        //创建一个通道
        Channel channel = connection.createChannel();

        //声明一个队列
//        channel.queueDeclare(QUEUE_NAME,false,false,false,null);

        //发送消息到队列中
        String message = "hello shangbaishuyao";

        for (int  i = 0; i< 1000; i++){
            channel.basicPublish("", QUEUE_NAME,null,(message + i).getBytes("UTF-8"));
            System.out.println("Producer Send +" + message + i);
        }

        //关闭通道和连接
        channel.close();
        connection.close();
    }
}
