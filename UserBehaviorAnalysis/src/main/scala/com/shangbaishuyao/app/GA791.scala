package com.shangbaishuyao.app

import java.time.ZoneId
import java.util.Properties

import com.shangbaishuyao.Handler.{Copy, LiuLiangUserDetailBucketAssigner, SendMail}
import com.shangbaishuyao.utils.{GA791_ProducerKafka, MyKafkaUtil2, PropertiesUtil}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.{SequenceFileWriter, StringWriter}
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.hadoop.io.{IntWritable, Text}

/**
 * Desc: GA791 <br/>
 * create by shangbaishuyao on 2021/3/31
 * @Author: 上白书妖
 * @Date: 14:06 2021/3/31
 */
object GA791 {
  def main(args: Array[String]): Unit = {
    //初始化环境变量
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //指定时间语义
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.streaming.api.scala._
    //读取kafka数据
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers",GA791_ProducerKafka.bootstraps)
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","consumer-group1")
    props.setProperty("auto.offset.reset","latest")
    //kafka配置
    val kafkaProps: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](
      GA791_ProducerKafka.GA791,
      new SimpleStringSchema(), props)

    val config: Properties = PropertiesUtil.load("config.properties")
    val errorRank: String = config.getProperty("error")

    //读取数据 , 并且设置时间语义和waterMark(水位线)
    //    env.readTextFile(getClass.getResource("").getPath)
    //83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
    //83.149.9.216 - - 17/05/2015:10:05:43 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png
    //添加数据源  测试的时候需要加上setStartFromEarliest()
//    val value: DataStream[String] = env.addSource(kafkaProps.setStartFromEarliest())
    val value: DataStream[String] = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\vmware.log")
//    value.writeAsText("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\hdfs.txt")
    val value1: DataStream[String] = value.map(
      line => {
        var a = "";
        //判断是否包含指定字符
        if (line.contains(errorRank)) {
          println(line)
//          SendMail.SendMail2("系统告警错误内容:" + line, "系统告警通知")
          a = line
//          hdfsSinkDemo.sinkHDFS3(line)
        }
        a
      }
    )
    println("======================"+value1+"=====================")
    //必须要设置,检查点10秒钟
    env.enableCheckpointing(10000);
    val sink: StreamingFileSink[String] = hdfsSinkDemo.sinkHDFS()
//    val sink: BucketingSink[String] = hdfsSinkDemo.sinkHDFS2()
//    val sink = new BucketingSink[String]("D:\\idea_out\\rollfilesink")
//    sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm", ZoneId.of("America/Los_Angeles")))
//    sink.setWriter(new StringWriter[String])
//    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB

//    value1.addSink(MyKafkaUtil2.getKafkaSink("t_ub"))
    value1.addSink(sink)
    Copy.putTxt()
//    Copy.test("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\out\\2021-05-22--19\\.part-0-0.inprogress.00449570-b345-492f-9c08-835901cf9d2f","/data/")
    env.execute("test")
  }
}

