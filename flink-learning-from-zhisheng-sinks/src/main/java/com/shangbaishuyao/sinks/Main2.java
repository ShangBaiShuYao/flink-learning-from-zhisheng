package com.shangbaishuyao.sinks;

import com.shangbaishuyao.sinks.sinks.MySink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc: 测试RichSink函数构造方法和开放函数 (test RichSink function construction method and open function)
 *
 * 个人理解:
 *    在执行环境中添加socket源,即source. 得到我们的DataStreamSource(流式数据源)获取数据,然后想办法将数据从数据源输出出去,所以使用addsink(添加水槽)将数据流出去.
 *    没有定义Sink我们首先得初始化,这里我们使用的是MySink.因为这里我是自定义的Sink 端.自定义的继承了RichSinkFunction函数,所以我们需要实现他的一些基本方法操作.
 *    通过这个"Flink data by socket to Console" 我们可以看清楚每个方法都有哪些作用.
 *
 *@Author: 上白书妖
 *@Date: 2020/11/25 11:53
 */
public class Main2 {
    public static void main(String[] args) throws Exception {
        //初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 输入端                    输出端
         *
         * source---->channel------>sink
         */
        //获取socket连接,数据源
        DataStreamSource<String> streamSource = env.socketTextStream("127.0.0.1", 7777);

        //并行度设置为5
        streamSource.addSink(new MySink("6")).setParallelism(5);

        //触发程序执行。该环境将执行导致“接收”操作的程序的所有部分。例如，Sink操作打印结果或将结果转发到消息队列。
        env.execute("Flink data by socket to Console");
    }
}
