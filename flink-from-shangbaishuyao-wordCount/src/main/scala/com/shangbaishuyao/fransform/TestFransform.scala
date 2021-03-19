package com.shangbaishuyao.fransform

import com.atguigu.flink.source.FromCustomerSource.MyCustomerSource
import com.atguigu.flink.source.SensorReader
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestFransform {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[SensorReader] = streamEnv.addSource(new MyCustomerSource)

    //1、根据传感器数据的编号分组，计算气温最高的值 （max聚合算子）
//    val stream2: KeyedStream[SensorReader, Tuple] = stream.keyBy("id")

//    val stream3: DataStream[SensorReader] = stream2.max("temperature")
//    stream3.print()

    //2、根据传感器数据的编号分组 ，每一组时间最近的，气温也是最高的数据
//    stream.keyBy(0).reduce( (s1,s2)=>{
//      new SensorReader(s1.id,if(s1.timestamp>s2.timestamp) s1.timestamp else  s2.timestamp,if(s1.temperature>s2.temperature) s1.temperature else s2.temperature)
//    }).print()


    //3、把温度按照0度拆分两个stream
    val stream2: SplitStream[SensorReader] = stream.split(s => {
      if (s.temperature >= 0) Seq("high") else Seq("low")
    }) //流并没有真正切割
    val highStream: DataStream[SensorReader] = stream2.select("high")
    val lowStream: DataStream[SensorReader] = stream2.select("low")


    //4、highStream 和 lowStream 连接成一个流  connect 1、允许两个流的类型不一致  2、并没有真实把两个流汇合
    val stream3: ConnectedStreams[SensorReader, SensorReader] = highStream.connect[SensorReader](lowStream)
    //把stream3中的sensorReader对象变成三元组
    val stream4: DataStream[(String, Long, Double)] = stream3.map(
      sensor => ((sensor.id, sensor.timestamp, sensor.temperature)),
      sensor => ((sensor.id, sensor.timestamp, sensor.temperature))
    )
    //connect 没有真正汇合，如果真正意义上的汇合需要再次调用map或者flatmap


    //5、union算子可以真正汇合
    val stream5: DataStream[SensorReader] = highStream.union(lowStream)
    stream5.print()


    streamEnv.execute()
  }
}
