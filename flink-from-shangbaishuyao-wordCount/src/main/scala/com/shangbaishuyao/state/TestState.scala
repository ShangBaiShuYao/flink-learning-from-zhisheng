package com.shangbaishuyao.state

import com.shangbaishuyao.source.SensorReader
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * Desc:
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 13:30 2021/3/20
 *
 * 监控每一个传感器，如果连续两个温度变化超过一个阈值（10），发出一个报警信息(三元组（id，前一个温度，当前的温度）)
 */
object TestState {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1) //默认情况下每个任务的并行度为1
    import org.apache.flink.streaming.api.scala._
    //读取netcat流中数据 （实时流） ,读取的数据都是SensorReader
    var stream1= streamEnv.socketTextStream("hadoop101",7777).map(line=>{
      val arr: Array[String] = line.split(",")
      new SensorReader(arr(0),arr(1).toLong,arr(2).toDouble)
    })
    stream1.print("input data")


    //第一种
//    stream1.keyBy(_.id).flatMap(new RichFlatMapFunction[SensorReader,(String,Double,Double)] {
    ////      var preState :ValueState[Double] =_
    ////
    ////      /**
    ////       *  1、第一条数据进来，之前没有保存前一个温度的状态
    ////       *  2、第二条以后的数据进来
    ////       * @param value
    ////       * @param out
    ////       */
    ////      override def flatMap(value: SensorReader, out: Collector[(String, Double, Double)]) = {
    ////        var preTemperature =preState.value();
    ////        var currentTemperature =value.temperature
    ////        if(preTemperature==0){
    ////          preState.update(currentTemperature)
    ////        }else {
    ////          var diff =(preTemperature-currentTemperature).abs
    ////          if(diff>=10)
    ////            out.collect((value.id,preTemperature,currentTemperature))
    ////          preState.update(currentTemperature)
    ////        }
    ////      }
    ////
    ////      //初始化状态
    ////      override def open(parameters: Configuration) = {
    ////        preState=getRuntimeContext.getState(new ValueStateDescriptor[Double]("pre_state",classOf[Double]))
    ////      }
    ////    }).print("main")

    //第二种
    stream1.keyBy(_.id).flatMapWithState[(String,Double,Double),Double]{
      //1、第一条数据进来，之前没有保存前一个温度的状态
      case (sensorReader: SensorReader,None)=>{(List.empty,Some[Double](sensorReader.temperature))}
        //第二种，第二条，和之后的数据进来
      case(sensorReader: SensorReader,state: Some[Double]) =>{
        var diff= (sensorReader.temperature-state.get).abs
        if(diff>=10)  //温度的变化超过了阈值
          (List((sensorReader.id,state.get,sensorReader.temperature)),Some[Double](sensorReader.temperature))
        else  //温度的变化没有超过阈值
          (List.empty,Some[Double](sensorReader.temperature))
      }
    }.print("main")

    streamEnv.execute()
  }
}
