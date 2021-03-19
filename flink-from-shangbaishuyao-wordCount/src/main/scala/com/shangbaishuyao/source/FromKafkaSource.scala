package com.shangbaishuyao.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * 从kafka中读取数据，当前的job作为kafka的消费者
 */
object FromKafkaSource {


  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    import org.apache.flink.streaming.api.scala._
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9092,hadoop103:9092")
    props.setProperty("group.id","fink01")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("auto.offset.reset","latest")

    //设置kafka为数据源
//    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("t_0615",new SimpleStringSchema(),props))
    //如果kafka中的数据是键值对
      val stream: DataStream[(String, String)] = env.addSource(new FlinkKafkaConsumer011[(String,String)]("t_0615",new KeyedDeserializationSchema[(String,String)](){

      //把字节数组变成字符串封装成二元组返回
      override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long) = {
        if(messageKey!=null && message!=null){
          val key = new String(messageKey,"UTF-8")
          val value = new String(message,"UTF-8")
          (key,value)
        }else{ //如果kafka中的数据为空返回一个固定的二元组
          ("null","null")
        }
      }

      override def isEndOfStream(nextElement: (String, String)) = {
        false
      }

      //定义返回的类型
      override def getProducedType = {
        createTuple2TypeInformation(createTypeInformation[String],createTypeInformation[String])
      }
    },props))  //setStartFromEarliest 第一次取数据的策略


    //转换算子
//    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    //设置sink
//    result.print("结果")
    stream.print()

    env.execute()
  }
}
