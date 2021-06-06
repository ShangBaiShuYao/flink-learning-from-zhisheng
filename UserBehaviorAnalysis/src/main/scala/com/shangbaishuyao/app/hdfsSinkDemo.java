package com.shangbaishuyao.app;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;
/*
 * Author: shangbaishuyao
 * Date: 21:46 2021/5/20
 * Desc: Flink 写入到HDFS
 */
public class hdfsSinkDemo {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<String> upper = lines.map(String::toUpperCase);

        //然后建将官网写好的代码复制到这个平台
        String patha = "D:\\test\\out";

        //必须要设置,检查点10秒钟
        env.enableCheckpointing(10000);

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(patha), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(30))//多长时间运行一个文件  秒
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(10))//多长时间没有写入就生成一个文件
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        upper.addSink(sink);
        env.execute();
    }
    public static StreamingFileSink<String> sinkHDFS() throws Exception{
        String patha = "H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\out\\20210523134425284.part-0-0";
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(patha), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(30))//多长时间运行一个文件  秒
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(10))//多长时间没有写入就生成一个文件
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();
        return sink;
    }
    public static BucketingSink<String> sinkHDFS2(){
        // 方式1：将数据导入Hadoop的文件夹
        //recordData.writeAsText("hdfs://hadoop:9000/flink/");
        // 方式2：将数据导入Hadoop的文件夹
        BucketingSink<String> hadoopSink = new BucketingSink<>("hdfs://hadoop102:8020/data");
        // 使用东八区时间格式"yyyy-MM-dd--HH"命名存储区
        hadoopSink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HH", ZoneId.of("Asia/Shanghai")));
        // 下述两种条件满足其一时，创建新的块文件
        // 条件1.设置块大小为100MB
        hadoopSink.setBatchSize(1024 * 1024 * 100L);
        // 条件2.设置时间间隔20min
        hadoopSink.setBatchRolloverInterval(20 * 60 * 1000L);
        // 设置块文件前缀
        hadoopSink.setPendingPrefix("s");
        // 设置块文件后缀
        hadoopSink.setPendingSuffix("x");
        // 设置运行中的文件前缀
        hadoopSink.setInProgressPrefix(".");
        // 添加Hadoop-Sink,处理相应逻辑
//        source.addSink(hadoopSink);
//        flinkEnv.execute();
        return hadoopSink;
    }

    public static void sinkHDFS3(String line) throws IOException {
        try {
            //将写入转化为流的形式
            BufferedWriter bw = new BufferedWriter(new FileWriter("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\hdfs.txt"));
            //一次写一行
//        String ss = "测试数据";
            bw.write(line);
            bw.newLine(); //换行用
            bw.flush();
            System.out.println("写入成功");
            bw.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
