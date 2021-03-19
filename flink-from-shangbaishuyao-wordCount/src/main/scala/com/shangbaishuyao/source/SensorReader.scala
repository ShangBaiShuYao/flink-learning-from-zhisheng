package com.shangbaishuyao.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 读取传感器数据的对象
 * @param id
 * @param timestamp 产生数据的时间，精确到秒
 * @param temperature
 */
case  class SensorReader(
                          id:String,timestamp:Long,
                          temperature:Double)


object FromCollectionSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[SensorReader] = env.fromCollection[SensorReader](List(
      new SensorReader("sensor_id1", System.currentTimeMillis() / 1000, 9.1),
      new SensorReader("sensor_id2", System.currentTimeMillis() / 1000, 23.1),
      new SensorReader("sensor_id3", System.currentTimeMillis() / 1000, 42.1)
    ))
    stream.print()
    env.execute()
  }
}

