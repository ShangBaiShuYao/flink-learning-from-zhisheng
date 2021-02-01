package com.shangbaishuyao.connectors.rabbitmq.model;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Desc:
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 21:46 2021/1/31
 */
public class EndPoint {
    protected Channel channel;
    protected Connection connection;
    protected String endPointName;

    public EndPoint(String endPointName)throws Exception{
        this.endPointName=endPointName;
        //便利的“工厂”类，方便打开一个{@link连接}到AMQP代理。
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost("127.0.0.1");
        factory.setUsername("admin");
        factory.setPassword("admin");
        factory.setPort(5672);

        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(endPointName,false,false,false,null);
    }

    /**
     * 关闭channel和connection。并非必须，因为隐含是自动调用的
     * @throws Exception
     */
    public void close() throws Exception{
        this.channel.close();
        this.connection.close();
    }
}
