package com.shangbaishuyao.demo.FlinkDemo06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
/**
 * Author: shangbaishuyao
 * Date: 0:23 2021/4/23
 * Desc: 案例
 */
public class Flink09_State_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));
        //开启CK
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //Cancel任务时不删除CK
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.读取端口数据并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = env.socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                });

        //3.按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);
        //4.计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        //5.打印
        result.print();
        //6.执行任务
        env.execute();
    }
}
