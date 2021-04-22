package com.shangbaishuyao.demo.FlinkDemo02;

import com.shangbaishuyao.demo.UDFSource.MySocketSourceUtil;
import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Source_MySource {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从自定义的数据源中加载数据
        DataStreamSource<WaterSensor> dataStreamSource = env.addSource(new MySocketSourceUtil("hadoop102", 9999));
        //3.打印结果数据
        dataStreamSource.print();
        //4.执行任务
        env.execute();
    }
}
