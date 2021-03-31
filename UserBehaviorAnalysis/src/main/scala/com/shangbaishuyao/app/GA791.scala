package com.shangbaishuyao.app

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.util
import java.util.Properties
import com.shangbaishuyao.Handler.writeHDFS
import com.shangbaishuyao.utils.{GA791_ProducerKafka, MyKafkaUtil}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

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

    val list = new util.ArrayList[String]()

    //读取数据 , 并且设置时间语义和waterMark(水位线)
    //    env.readTextFile(getClass.getResource("").getPath)
    //83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
    //83.149.9.216 - - 17/05/2015:10:05:43 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png
    //添加数据源  测试的时候需要加上setStartFromEarliest()
    env.addSource(kafkaProps.setStartFromEarliest())
      //    env.readTextFile("H:\\大数据电商数仓\\Bigdata_warehouse_Hadoop104\\vmware.log")
      .map(
        line => {
          val arr: Array[String] = line.split(" ")
          if (arr.contains("error")) {
            //            println(arr.toList)
            println(arr.toList.toString())
            val file = new File("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\hdfs.txt")
            val writer_name = new BufferedWriter(new FileWriter(file))
//            val writer = new PrintWriter(new File("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\hdfs.txt"))
            val list1: List[String] = arr.toList
            for (elem <- list1) {
//              println(elem)
              writer_name.write(elem)
            }

            Thread.sleep(1000)
            //               sendSMS.send2("19956571280",null)
          }
          if (arr.contains("ERROR")) {
            val file = new File("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\hdfs.txt")
            val writer_name = new BufferedWriter(new FileWriter(file))
            println(arr.toList.toString())
            val list1: List[String] = arr.toList
            for (elem <- list1) {
//              println(elem)
              writer_name.write(elem)
            }

            //               sendSMS.send2("19956571280",null)
//            arr.toList.foreach(x => list.add(x))
          }
        }
      )
    writeHDFS.testCopyFromLocalFile
    //执行
    env.execute()
  }
}

