package com.shangbaishuyao.demo.FlinkDemo01;

import com.shangbaishuyao.demo.Function.MyFlatMapFunc2;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 * @author: 徐霸天
 * @des:
 * @date: 上午10:56 2021/12/5
 **/
public class Flink_WordCount_Bounded {
    public static void main(String[] args) throws Exception{
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取文件创建流
        DataStreamSource<String> input = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\flink-1.12.0-Demo\\input\\word.txt");
        //3.压平并将单词转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new MyFlatMapFunc2());
        //4.分组  TODO 批处理里面有groupBy , 但是流里面没有 ,只能用keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0; //scala的第一个元素时f_1,但是java是f0
                    }
                });
        //5.按照Key做聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        //6.打印结果
        result.print();
        //7.启动任务
        env.execute("Flink_WordCount_Bounded");
    }
}
