package com.shangbaishuyao.configration.apollo;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * Desc: 整合 flink apollo
 * create by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 11:42 2021/1/19
 */
@Slf4j
public class FlinkApolloTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(1);

        env.addSource(new RichSourceFunction<String>() {
            private Config config;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                config = ConfigService.getAppConfig(); //获取Apollo配置
                //设置监听器
                config.addChangeListener(new ConfigChangeListener() {
                    @Override
                    public void onChange(ConfigChangeEvent configChangeEvent) {
                        for (String key : configChangeEvent.changedKeys()){
                            ConfigChange change = configChangeEvent.getChange(key);
                            log.info("key: {}, oldValue: {}, newValue: {}, changeType: {}:",
                                      change.getPropertyName(),change.getOldValue(),change.getNewValue(),change.getChangeType()
                                    );
                        }
                    }
                });
            }

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true){
                    sourceContext.collect(config.getProperty("name","shangbaishuyao"));
                    Thread.sleep(300);
                }
            }

            @Override
            public void cancel() {

            }
        }).print();

        env.execute("shangbaishuyao flink Apollo");
    }
}
