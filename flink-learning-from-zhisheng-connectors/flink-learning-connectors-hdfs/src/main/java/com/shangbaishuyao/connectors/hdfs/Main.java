package com.shangbaishuyao.connectors.hdfs;

import com.shangbaishuyao.common.model.MetricEvent;
import com.shangbaishuyao.common.utils.ExecutionEnvUtil;
import com.shangbaishuyao.common.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc:
 * create by shangbaishuyao on 2021/1/28
 * @Author: 上白书妖
 * @Date: 0:36 2021/1/28
 */
public class Main {
    public static void main(String[] args) throws Exception{

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);


        env.execute("flink learning connectors hdfs");
    }
}
