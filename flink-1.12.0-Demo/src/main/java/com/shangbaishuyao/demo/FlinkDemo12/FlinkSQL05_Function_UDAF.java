package com.shangbaishuyao.demo.FlinkDemo12;

import com.shangbaishuyao.demo.bean.SumCount;
import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
/*
 * Author: shangbaishuyao
 * Date: 13:46 2021/4/23
 * Desc: 自定义UDAF函数
 */
public class FlinkSQL05_Function_UDAF {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS);
        //4.先注册再使用
        tableEnv.createTemporarySystemFunction("myavg", MyAvg.class);

        //TableAPI
//        table.groupBy($("id"))
//                .select($("id"),call("myavg",$("vc")))
//                .execute()
//                .print();

        //SQL
        tableEnv.sqlQuery("select id,myavg(vc) from " + table + " group by id")
                .execute().print();

        //6.执行任务
        env.execute();

    }
    public static class MyAvg extends AggregateFunction<Double, SumCount> {
        @Override
        public SumCount createAccumulator() {
            return new SumCount();
        }
        public void accumulate(SumCount acc, Integer vc) {
            acc.setVcSum(acc.getVcSum() + vc);
            acc.setCount(acc.getCount() + 1);
        }
        @Override
        public Double getValue(SumCount accumulator) {
            return accumulator.getVcSum() * 1D / accumulator.getCount();
        }
    }
}
