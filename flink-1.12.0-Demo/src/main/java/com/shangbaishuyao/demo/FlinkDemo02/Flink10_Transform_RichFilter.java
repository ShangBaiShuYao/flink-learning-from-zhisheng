package com.shangbaishuyao.demo.FlinkDemo02;

import com.shangbaishuyao.demo.Function.MyRichFilterFunc;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * Author: shangbaishuyao
 * Date: 16:16 2021/4/22
 * Desc:
 */
public class Flink10_Transform_RichFilter {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2.从文件读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\flink-1.12.0-Demo\\input\\sensor.txt");
        //3.过滤数据,只取水位高于30的
        SingleOutputStreamOperator<String> result = stringDataStreamSource.filter(new MyRichFilterFunc());
        //4.打印数据
        result.print();
        //5.执行任务
        env.execute();
    }
}
