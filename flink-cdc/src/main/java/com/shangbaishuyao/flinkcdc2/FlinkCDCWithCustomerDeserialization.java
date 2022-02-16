package com.shangbaishuyao.flinkcdc2;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/*
 * @author: shangbaishuyao
 * @des:
 * @date: 上午12:09 2022/2/17
 **/
public class FlinkCDCWithCustomerDeserialization {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("shangbaishuyao")
                .databaseList("xubatian")
                .tableList("xubatian.base_trademark")   //如果不添加该参数,则消费指定数据库中所有表的数据.如果指定,指定方式为db.table
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        //3.打印数据
        streamSource.print();
        //4.启动任务
        env.execute("FlinkCDCWithCustomerDeserialization");

    }

}
