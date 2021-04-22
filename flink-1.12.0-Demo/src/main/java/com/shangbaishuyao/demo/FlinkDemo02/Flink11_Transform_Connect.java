package com.shangbaishuyao.demo.FlinkDemo02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
/**
 * Author: shangbaishuyao
 * Date: 16:16 2021/4/22
 * Desc:
 */
public class Flink11_Transform_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据创建流
        DataStreamSource<String> stringDS = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> socketTextStream2 = env.socketTextStream("hadoop102", 9999);
        //3.将socketTextStream2转换为Int类型
        SingleOutputStreamOperator<Integer> intDS = socketTextStream2.map(String::length);
        //4.连接两个流
        ConnectedStreams<String, Integer> connectedStreams = stringDS.connect(intDS);
        //5.处理连接之后的流
        SingleOutputStreamOperator<Object> result = connectedStreams.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String value) throws Exception {
                return value;
            }
            @Override
            public Object map2(Integer value) throws Exception {
                return value;
            }
        });
        //6.打印数据
        result.print();
        //7.执行
        env.execute();
    }
}
