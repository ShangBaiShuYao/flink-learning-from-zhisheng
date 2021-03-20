package com.shangbaishuyao.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import scala.util.Random
/**
 * Desc:
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 1:15 2021/3/20
 */
object FromCustomerSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[SensorReader] = env.addSource(new MyCustomerSource)
    stream.print()
    env.execute()
  }
}


