package com.shangbaishuyao.connectors.es6.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc: ES Sink utils（get ES host、addSink）//todo: index template & x-pack <br/>
 * create by shangbaishuyao on 2021/2/1
 * @Author: 上白书妖
 * @Date: 14:03 2021/2/1
 */
public class ESSinkUtil {
    //es security constant (es安全常数)
    public static final String ES_SECURITY_ENABLE = "es.security.enable";
    public static final String ES_SECURITY_USERNAME = "es.security.username";
    public static final String ES_SECURITY_PASSWORD = "es.security.password";


    /**
     * Desc: es sink
     *
     * @param hosts               es hosts
     * @param bulkFlushMaxActions bulk flush size
     * @param parallelism         并行度
     * @param data                数据
     * @param function
     * @param parameterTool
     * @param <T>
     */
    public static <T> void addSink(List<HttpHost> hosts,
                                   int bulkFlushMaxActions,
                                   int parallelism,
                                   SingleOutputStreamOperator<T> data,
                                   ElasticsearchSinkFunction<T> function,
                                   ParameterTool parameterTool){
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts, function);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        esSinkBuilder.setFailureHandler(new RetryRequestFailureHandler());
        //todo:xpack security
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }

    /**
     * 解析配置文件的 es hosts
     *
     * @param hosts
     * @return
     * @throws MalformedURLException
     */
    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        ArrayList<HttpHost> addresses = new ArrayList<>();

        for (String host : hostList) {
            //startsWith: 测试该字符串是否以指定的前缀开头
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }

}
