package com.shangbaishuyao.configration.nacos;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Desc: 测试 nacos 动态修改 CheckPoint 配置后,Flink 是否可以获取到更改后的值, 并且生效. <br/>
 *
 * 结论:
 *      不生效 , 因为Flink 是 Lazy Evaluate(延迟执行), 当程序的 main 方法执行时, 数据源加载数据和数据转换等算子不会立马执行,
 *      这些操作会被创建并添加到程序的执行计划中去,只有当执行环境 env 的 execute 方法被显示的触发执行时, 整个程序才开始执行实际操作,
 *      所以, 在一开始初始化后等程序执行 execute() 方法后再修改 env 的配置其实就不起作用了.
 *
 * create by shangbaishuyao on 2021-01-17
 * @Author: 上白书妖
 * @Date: 10:17 2021/1/19
 */
public class FlinkNacosTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(1);

        //nacos配置
        String serverAddr = "localhost"; //服务器地址
        String dataId = "test";
        String group = "DEFAULT_GROUP";

        Properties properties = new Properties();
        properties.setProperty("serverAddr",serverAddr);

        ConfigService configService = NacosFactory.createConfigService(properties);
        final  String[] content = {configService.getConfig(dataId, group, 5000)};

        //设置监听器
        configService.addListener(dataId, group, new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String configInfo) {
                System.out.println("setCheckPointInterval start");
                content[0] = configInfo;
                //设置checkPoint间隔  单词: Interval 间隔
                env.getCheckpointConfig().setCheckpointInterval(Long.parseLong(content[0]));
                System.out.println("setCheckPointInterval over");
                System.out.println("Look Look print checkPointInterval:"+env.getCheckpointConfig().getCheckpointInterval());
            }
        });
        System.out.println("Look Look print content[0]:"+content[0]);

        env.getCheckpointConfig().setCheckpointTimeout(1000);
        env.getCheckpointConfig().setCheckpointInterval(Long.parseLong(content[0]));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); //exactly-once(精确一致,这是属于最高级别): 这指的是系统保证在发生故障后得到的计数结果与正确值一致。当然,结果绝对一致的话,那我们的状态就绝对一致的.

        //添加数据源到环境变量
        env.addSource(new SourceFunction<Tuple2<String,Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
                while (true){
                    sourceContext.collect(new Tuple2<>("shangbaishuyao",System.currentTimeMillis()));
                    Thread.sleep(800);
                }
            }

            @Override
            public void cancel() {

            }
        }).print();

        env.execute("测试 nacos 动态修改 CheckPoint 配置后,Flink 是否可以获取到更改后的值, 并且生效");
    }
}
