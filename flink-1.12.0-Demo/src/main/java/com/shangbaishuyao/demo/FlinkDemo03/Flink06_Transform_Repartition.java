package com.shangbaishuyao.demo.FlinkDemo03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * Author: shangbaishuyao
 * Date: 16:45 2021/4/22
 * Desc:
 */
public class Flink06_Transform_Repartition {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        //2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<String> map = socketTextStream.map(x -> x).setParallelism(2);
        map.print("Map").setParallelism(2);
        map.rebalance().print("Rebalance");
        map.rescale().print("Rescale");
        //3.使用不同的重分区策略分区后打印
//        socketTextStream.keyBy(data -> data).print("KeyBy");
//        socketTextStream.shuffle().print("Shuffle");
//        socketTextStream.rebalance().print("Rebalance");
//        socketTextStream.rescale().print("Rescale");
//        socketTextStream.global().print("Global");
//        socketTextStream.forward().print("Forward"); // 报错
//        socketTextStream.broadcast().print("Broadcast");
        //4.开启任务
        env.execute();
    }
}
