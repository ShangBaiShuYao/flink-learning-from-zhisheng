package com.shangbaishuyao.demo.FlinkDemo03;

import com.shangbaishuyao.demo.UDFSink.MyMysqlSink;
import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * Author: shangbaishuyao
 * Date: 16:44 2021/4/22
 * Desc:
 */
public class Flink10_Sink_MySink {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]));
                    }
                });

        //3.将数据写入Mysql
        waterSensorDS.addSink(new MyMysqlSink());
        //4.执行任务
        env.execute();
    }
}
