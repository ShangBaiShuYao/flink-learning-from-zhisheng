package com.shangbaishuyao.configration.nacos;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Desc: flink nacos 整合测试,nacos直接做配置中心动态更新配置<br/>
 * create by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 9:13 2021/1/19
 */
public class FlinkNacosTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(1);

        //nacos的相关配置
        String serverAddr = "localhost";
        String dataId = "test";
        String group = "DEFAULT_GROUP";

        Properties properties = new Properties();
        properties.put("serverAddr",serverAddr);

        ConfigService configService = NacosFactory.createConfigService(properties);
        String content = configService.getConfig(dataId, group, 5000);
        System.out.println("main" + content);


        //添加数据源
        env.addSource(new RichSourceFunction<String>() {
            ConfigService configService;
            String config;
            String dataId ="test";
            String group ="DEFAULT_GROUP";

            //open 初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                String serverAddress = "localhost";
                Properties properties = new Properties();
                properties.setProperty("serverAddr", serverAddress);

                configService = NacosFactory.createConfigService(properties);
                config = configService.getConfig(dataId, group, 5000);

                //添加监听器
                configService.addListener(dataId, group, new Listener() {
                    @Override
                    public Executor getExecutor() {
                        return null;
                    }

                    @Override
                    public void receiveConfigInfo(String configInfo) {
                        config = configInfo;
                        System.out.println("open Listener receiver / 打开监听器接收: " + configInfo);
                    }
                });
            }

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true){
                    Thread.sleep(3000);
                    System.out.println("run config =" + config);
                    sourceContext.collect(String.valueOf(System.currentTimeMillis()));
                }
            }

            @Override
            public void cancel() {}
        }).print();

        env.execute("shangbaishuyao flink nacos 整合配置中心动态配置");
    }
}
