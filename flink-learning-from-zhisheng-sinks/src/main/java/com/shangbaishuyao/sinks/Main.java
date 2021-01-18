package com.shangbaishuyao.sinks;

import com.shangbaishuyao.common.constant.PropertiesConstants;
import com.shangbaishuyao.common.utils.ExecutionEnvUtil;
import com.shangbaishuyao.common.utils.GsonUtil;
import com.shangbaishuyao.common.utils.KafkaConfigUtil;
import com.shangbaishuyao.sinks.model.Student;
import com.shangbaishuyao.sinks.sinks.SinkToMySQL;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
/**
 * Desc: 将kafka里面的数据写入答mysql数据库中<br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/25 11:46
 */
public class Main {
    public static void main(String[] args) throws Exception {
       final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 几乎所有的Flink应用程序，包括批处理和流处理，都依赖于外部配置参数，这些参数被用来指定输入和输出源(如路径或者地址)，系统参数(并发数，运行时配置)和应用程序的可配参数(通常用在自定义函数中)。 <br/>
         * Flink提供了一个简单的叫做ParameterTool的utility，ParameterTool提供了一些基础的工具来解决这些问题，当然你也可以不用这里所有描述的ParameterTool，                             <br/>
         * 其他框架如:Commons CLI和argparse4j在Flink中也是支持的。                                                                                                         <br/>
         */
        //使用已经定义好的工具类 ExecutionEnvUtil(执行执行环境工具类去读取我resource下的../application.properties配置文件)得到基础工具类
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;

        //使用我们自定义号的kafka工具类去读物我们设置的parameterTool,因为在上面我们的parameterTool里面设置了来自resource下的../application.properties里面的配置文件了
        //这就是完整的kafka配置了
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);

        //执行环境中添加数据源
        DataStreamSource<String> dataStreamSource = env.addSource(//在执行环境中添加kafka消费者数据源
                new FlinkKafkaConsumer011<>( //为Kafka 0.11.x创建一个新的Kafka流媒体源消费者。
                        parameterTool.get(PropertiesConstants.METRICS_TOPIC), //这个 kafka topic需要和上面的工具类的topic 一致
                        new SimpleStringSchema(),//用来在Kafka的字节消息和Flink的对象之间进行转换的反/序列化器
                        props //用于配置Kafka客户端和ZooKeeper客户端的属性。
                )
        ).setParallelism(1);

        //map 分散      reduce 合并                                    <br> 解析  字符串  转变成  student 对象 <br/>
        SingleOutputStreamOperator<Student> student = dataStreamSource.map(string -> GsonUtil.fromJson(string, Student.class)); //这里用的是gson解析，解析字符串成 student 对象

        //数据 sink 到 mysql
        student.addSink(new SinkToMySQL());

        //触发程序执行。该环境将执行导致“接收”操作的程序的所有部分。例如，Sink操作打印结果或将结果转发到消息队列。
        env.execute("Flink data Sink");
    }
}
