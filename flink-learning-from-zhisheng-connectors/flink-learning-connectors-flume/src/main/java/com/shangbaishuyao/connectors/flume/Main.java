package com.shangbaishuyao.connectors.flume;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 * Desc:
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 12:54 2021/1/31
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.execute("flink learning project template");
    }
}
