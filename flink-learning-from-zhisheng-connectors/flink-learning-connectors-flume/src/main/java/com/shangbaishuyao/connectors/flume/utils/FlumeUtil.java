package com.shangbaishuyao.connectors.flume.utils;

import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;

import java.util.Properties;

/*
 * Desc:
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 12:54 2021/1/31
 */
public class FlumeUtil {
    private static final String CLIENT_TYPE_KEY = "client.type"; //客户端类型
    private static final String CLIENT_TYPE_DEFAULT_FAILOVER = "default_failover"; //默认故障切换
    private static final String CLIENT_TYPE_DEFAULT_LOADBALANCING = "default_loadbalance"; //默认负载均衡

    //RpcClient : 公共客户端接口发送数据到Flume
    public static RpcClient getRpcClient(String clientType, String hostname, Integer port, Integer batchSize) {
        Properties props;
        RpcClient client;
        switch(clientType.toUpperCase()) {
            //精简方式
            case "THRIFT":
                client = RpcClientFactory.getThriftInstance(hostname, port, batchSize);
                break;
            //默认方式
            case "DEFAULT":
                client = RpcClientFactory.getDefaultInstance(hostname, port, batchSize);
                break;
            //默认故障转移
            case "DEFAULT_FAILOVER":
                props = getDefaultProperties(hostname, port, batchSize);
                props.put(CLIENT_TYPE_KEY, CLIENT_TYPE_DEFAULT_FAILOVER);
                client = RpcClientFactory.getInstance(props);
                break;
             //默认负载均衡
            case "DEFAULT_LOADBALANCE":
                props = getDefaultProperties(hostname, port, batchSize);
                props.put(CLIENT_TYPE_KEY, CLIENT_TYPE_DEFAULT_LOADBALANCING);
                client = RpcClientFactory.getInstance(props);
                break;
            default:
                throw new IllegalStateException("Unsupported client type - cannot happen/不支持的客户端类型-不能发生");
        }
        return client;
    }


    //销毁方法
    public static void destroy(RpcClient client) {
        if (null != client) {
            client.close();
        }
    }

    //获取默认配置方法                                       主机名           端口号          批处理大小
    private static Properties getDefaultProperties(String hostname, Integer port, Integer batchSize) {
        Properties props = new Properties();
        //RpcClientConfigurationConstants: RpcClient使用的配置常量。这些配置钥匙可以通过属性对象指定适当的方法以获得一个定制的RPC客户端。
        props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
        props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1", hostname + ":" + port.intValue());
        props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, batchSize.toString());
        return props;
    }
}
