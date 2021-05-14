package com.shangbaishuyao.utils

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.BufferedSource


/**
 * Desc:全局的常量和公共函数<br/>
 * create by shangbaishuyao on 2021/3/20
 *
 * @Author: 上白书妖
 * @Date: 17:32 2021/3/20
 */ 
object GA783_ProducerKafka {
  val bootstraps:String="hadoop102:9092,hadoop103:9092,hadoop104:9092"
  val userBehaviorTopic:String="t_ub" // kafka中存放用户行为数据的主题
//  val vm = "vm";

  /**
   * 把数据导入kafka
   * @param topic
   * @param filePath
   */
  def writeDataToKafka(topic:String,filePath:String): Unit ={
    val props = new Properties()
    props.setProperty("bootstrap.servers",bootstraps)
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)
    //创建一个kafka的生产者
    val producer = new KafkaProducer[String,String](props)
    //读取文件内容
    val buffered: BufferedSource = scala.io.Source.fromFile(filePath)
    for(line<-buffered.getLines()){
      //数据发给kafka
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }
    producer.close()
  }
  /**
   * test
   * @param args
   */
  def main(args: Array[String]): Unit = {
//    while (true){
    //导入热门商品统计模块的数据
    writeDataToKafka(userBehaviorTopic,
      "H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\UserBehavior.csv")
//      Thread.sleep(1000)
//    }
  }
}
