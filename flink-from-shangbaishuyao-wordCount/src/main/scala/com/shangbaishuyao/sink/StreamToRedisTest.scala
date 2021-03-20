package com.shangbaishuyao.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

object StreamToRedisTest {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1) //默认情况下每个任务的并行度为1
    import org.apache.flink.streaming.api.scala._
    //读取netcat流中数据 （实时流）
    val stream1: DataStream[String] = streamEnv.socketTextStream("hadoop101",7777)
    //键值对的数据写入kafka
    val stream2: DataStream[(String, String)] = stream1.map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0), arr(1))
    })
    stream2
    //连接redis的配置
    val config: FlinkJedisPoolConfig = new  FlinkJedisPoolConfig.Builder().setDatabase(3).setHost("hadoop101").setPort(6379).build()

    //写入redis
    stream2.addSink(new RedisSink[(String, String)](config,new RedisMapper[(String, String)] {
      override def getCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"t_0615")
      override def getKeyFromData(data: (String, String)) = {
        data._1
      }
      override def getValueFromData(data: (String, String)) = {
        data._2
      }
    }))
    streamEnv.execute()
  }
}
