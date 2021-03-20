package com.shangbaishuyao.window

import com.shangbaishuyao.source.SensorReader
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * Desc:
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 13:21 2021/3/20
 */
object TestWindowAndWaterMarks {
  //1574840003
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1) //默认情况下每个任务的并行度为1
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //使用EventTime作为时间语义
    import org.apache.flink.streaming.api.scala._
    //读取netcat流中数据 （实时流） ,读取的数据都是SensorReader
    var stream1= streamEnv.socketTextStream("hadoop101",7777).map(line=>{
      val arr: Array[String] = line.split(",")
      new SensorReader(arr(0),arr(1).toLong,arr(2).toDouble)
    })
      stream1.print("输入的数据")

//      .assignAscendingTimestamps(_.timestamp) //数据进入的是本身就是按照EventTime严格升序的。参数代表：当前数据哪个是EventTime
//      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor(t))  如果考虑无序的情况下使用watermarks，第一种写法
      stream1=stream1.assignTimestampsAndWatermarks(new MyAssignerPeriodicWaterMarks) //如果考虑无序的情况下使用watermarks，第二种写法

    //需求：每隔5秒，统计最近10秒中的温度最大值(要求：滚动(滑动)窗口，使用EventTime)
    //1、先申明时间语义为EventTime
    //2、是否考虑数据是有序还是（无序） ，如果数据可能是按照EventTime无序进入的，所以需要引入WaterMarks
//    stream1.keyBy(_.id).window(TumblingEventTimeWindows.of(Time.seconds(10)))
//    stream1.keyBy(_.id).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
    stream1.keyBy(_.id).timeWindow(Time.seconds(10),Time.seconds(5))
      .reduce((s1,s2)=>{
        if(s1.temperature>s2.temperature) s1 else s2
      }).print("最大的温度")

    streamEnv.execute()
  }

  /**
   * 自定义的一个周期性配置水位线
   */
  class MyAssignerPeriodicWaterMarks extends AssignerWithPeriodicWatermarks[SensorReader]{
    var t:Long=2000 //设置当前的延迟时间为2秒

    var maxEventTime :Long= _ //当前最大的EventTime

    //获取当前的水位线,默认每隔200毫秒执行一次
    override def getCurrentWatermark: Watermark = {
      new Watermark(maxEventTime-t)
    }

    //获取当前数据对应的EventTime: 前面的代码中设置了EventTime的时间语义，现在需要指定具体数据中到哪个是EventTime
    override def extractTimestamp(element: SensorReader, previousElementTimestamp: Long): Long = {
      var nowEventTime:Long =element.timestamp*1000
      maxEventTime=maxEventTime.max(nowEventTime)
      nowEventTime  //返回具体数据中的EventTime
    }
  }
}
