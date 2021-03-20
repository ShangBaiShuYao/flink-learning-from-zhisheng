package com.shangbaishuyao.source

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import scala.util.Random
/**
 * Desc:
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 12:20 2021/3/20
 */
object MyKafkaProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("key.serializer",classOf[StringSerializer].getName)
    props.setProperty("value.serializer",classOf[StringSerializer].getName)
    val p = new KafkaProducer[String,String](props)
    val r = new Random()
    while(true){

      val data = new ProducerRecord[String,String]("t_0615","hello"+r.nextInt(10),r.nextInt()+"")
      p.send(data)
      Thread.sleep(2000)
    }
    p.close()
  }
}
