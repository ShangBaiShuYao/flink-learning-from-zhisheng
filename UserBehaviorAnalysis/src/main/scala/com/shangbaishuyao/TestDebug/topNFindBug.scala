package com.shangbaishuyao.TestDebug

import java.util.{Date, Properties}

import com.shangbaishuyao.bean.UserBehavior
import com.shangbaishuyao.utils.{MyKafkaUtil, MyKafkaUtil2}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Desc: 测试找bug , 这个类里面是对的 <br/>
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 20:26 2021/3/20
 */
object topNFindBug {
  def main(args: Array[String]): Unit = {
      //1、初始化环境
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      import org.apache.flink.streaming.api.scala._
      //指定时间语义为事件时间
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      //2、读取kafka数据
      val props = new Properties()
      props.setProperty("bootstrap.servers",MyKafkaUtil.bootstraps)
      props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
      props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
      props.setProperty("group.id","consumer-group1")
      props.setProperty("auto.offset.reset","latest")
      val stream: DataStream[UserBehavior] = env.addSource(new FlinkKafkaConsumer[String](MyKafkaUtil.userBehaviorTopic, new SimpleStringSchema(), props).setStartFromEarliest())
        .map(line => {
          val arr: Array[String] = line.split(",")
          new UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
        }).assignAscendingTimestamps(_.timestamp*1000) //指定EventTime具体是什么
      //3、
      val stream2: DataStream[(Long, Long)] = stream.filter(_.behavior.equals("pv"))
        .keyBy(_.itemId)
        .timeWindow(Time.hours(1), Time.minutes(5))
        .aggregate(new CountAggregate, new HotItemWindowFunction)  //聚合函数中，需要两个参数，第一个参数是做累加 ,第二个参数
      //4、把上一个窗口的结果数据做全量的排序计算
      stream2.timeWindowAll(Time.minutes(5))
        .process(new HotItemAllWindowFunction(3))
        .print()
      env.execute()
    }



  //根据商品ID分组累加每个商品的访问次数
  class CountAggregate extends AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = {0}
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator+1
    override def getResult(accumulator: Long): Long = accumulator
    override def merge(a: Long, b: Long): Long = a+b
  }

  //累加之后，把累加的结果输出 二元组(商品ID，该商品的访问次数)
  class HotItemWindowFunction extends WindowFunction[Long,(Long,Long),Long,TimeWindow]{
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[(Long, Long)]): Unit = {
      var itemId:Long=key
      val count: Long = input.iterator.next()
      out.collect((itemId,count))
    }
  }

  //全窗口聚合排序类
  class HotItemAllWindowFunction(topN:Int) extends ProcessAllWindowFunction[(Long,Long),String,TimeWindow]{
    override def process(context: Context, elements: Iterable[(Long, Long)], out: Collector[String]): Unit = {
      //从Iterable 得到数据按照访问的次数降序排序
      val tuples: List[(Long, Long)] = elements.toList.sortBy(_._2)(Ordering.Long.reverse).take(topN)
      var sb :StringBuilder =new StringBuilder()
      sb.append(" 当前一个小时内的数据前 "+topN +"个  窗口时间："+ new Date(context.window.getEnd) +" \n")
      tuples.foreach(t=>{
        sb.append("商品ID："+t._1+",访问的次数是: "+t._2+"\n")
      })
      sb.append("----------------------------------\n")
      out.collect(sb.toString())
    }
  }
}
