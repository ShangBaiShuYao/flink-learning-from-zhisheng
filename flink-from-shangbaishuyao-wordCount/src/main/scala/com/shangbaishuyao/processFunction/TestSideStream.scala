package com.shangbaishuyao.processFunction

import com.shangbaishuyao.source.{MyCustomerSource, SensorReader}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * Desc:
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 14:27 2021/3/20
 *
 * 业务：根据温度是否为零下温度还是零度以上，来输出侧流（零下温度） ,主流里面放零度以上的温度
 */
object TestSideStream {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[SensorReader] = streamEnv.addSource(new MyCustomerSource)

    //一个侧流需要定义一个标签
    var tag:OutputTag[SensorReader] =new OutputTag[SensorReader]("low")
    val highStream: DataStream[SensorReader] = stream.process(new MyCustomerProcessFunction(tag))

    //stream2 是主流

    //得到测流
    val lowStream: DataStream[SensorReader] = highStream.getSideOutput(tag)

    highStream.print("high")
    lowStream.print("low")

    streamEnv.execute()

  }

  class MyCustomerProcessFunction(tag:OutputTag[SensorReader]) extends ProcessFunction[SensorReader,SensorReader]{

    override def processElement(value: SensorReader, ctx: ProcessFunction[SensorReader, SensorReader]#Context, out: Collector[SensorReader]): Unit = {
      var temperature =value.temperature;
      if(temperature>=0){ //输出主流
        out.collect(value)
      }else{//输出侧流
        ctx.output(tag,value)
      }
    }
  }
}
