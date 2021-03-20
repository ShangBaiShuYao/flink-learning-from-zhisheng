package com.shangbaishuyao.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
/**
 * Desc:
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 13:30 2021/3/20
 */
object StreamToKafkaTest {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1) //默认情况下每个任务的并行度为1

    import org.apache.flink.streaming.api.scala._
    //读取netcat流中数据 （实时流）
    val stream1: DataStream[String] = streamEnv.socketTextStream("hadoop101",7777)

    //把数据写入kafka，并且不是键值对的
//    stream1.addSink( new FlinkKafkaProducer011[String]("hadoop101:9092,hadoop102:9092,hadoop103:9092","t_0615",new SimpleStringSchema()))

    //键值对的数据写入kafka
    stream1.map(line=>{
      val arr: Array[String] = line.split(",")
      (arr(0),arr(1))
    }).addSink(new FlinkKafkaProducer011[(String, String)]("hadoop101:9092,hadoop102:9092,hadoop103:9092"
    ,"t_0615", new KeyedSerializationSchema[(String, String)](){
        //key的序列化
        override def serializeKey(element: (String, String)) = {
          element._1.getBytes("UTF-8")
        }

        //值的序列化
        override def serializeValue(element: (String, String)) = {
          element._2.getBytes("UTF-8")
        }

        //可以根据不同的数据特点，写入到不同的topic中
        override def getTargetTopic(element: (String, String)) = {
          "t_0615"
        }
      }))
    streamEnv.execute()
  }
}
