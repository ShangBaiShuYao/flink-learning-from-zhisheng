package com.shangbaishuyao.connectors.activemq;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc: flink activemq
 * create by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 12:14 2021/1/19
 */
public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.execute("shangbaishuyao flink activemq template");
    }
}
