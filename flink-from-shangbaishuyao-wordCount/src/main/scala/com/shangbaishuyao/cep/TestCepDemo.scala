package com.shangbaishuyao.cep

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 登录告警系统
 * 从一堆的登录日志从，匹配一个恶意登录的模式（如果一个用户连续失败三次，则是恶意登录），从而找到哪些用户名是用于恶意 登录
 */

case class EventLog(id:Long,userName:String,eventType:String,eventTime:Long)

object TestCepDemo {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)

    import org.apache.flink.streaming.api.scala._
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream: DataStream[EventLog] = streamEnv.fromCollection(List(
      new EventLog(1, "张三", "fail", 1574840003),
      new EventLog(1, "张三", "fail", 1574840004),
      new EventLog(1, "张三", "fail", 1574840005),
      new EventLog(2, "李四", "fail", 1574840006),
      new EventLog(2, "李四", "sucess", 1574840007),
      new EventLog(1, "张三", "fail", 1574840008)
    )).assignAscendingTimestamps(_.eventTime * 1000)
    stream.print("input data")
    //定义模式
    val pattern: Pattern[EventLog, EventLog] = Pattern.begin[EventLog]("begin").where(_.eventType.equals("fail"))
      .next("next1").where(_.eventType.equals("fail"))
      .next("next2").where(_.eventType.equals("fail"))
      .within(Time.seconds(10))

    //cep 做模式检测
    val patternStream: PatternStream[EventLog] = CEP.pattern[EventLog](stream.keyBy(_.id),pattern)

    //第三步: 输出alert

    val result: DataStream[String] = patternStream.select(new PatternSelectFunction[EventLog, String] {
      override def select(map: util.Map[String, util.List[EventLog]]) = {
        val iter: util.Iterator[String] = map.keySet().iterator()
        val e1: EventLog = map.get(iter.next()).iterator().next()
        val e2: EventLog = map.get(iter.next()).iterator().next()
        val e3: EventLog = map.get(iter.next()).iterator().next()

        "id:" + e1.id + " 用户名:" + e1.userName + "登录的时间:" + e1.eventTime + ":" + e2.eventTime + ":" + e3.eventTime
      }
    })

    result.print(" main ")

    streamEnv.execute()
  }
}
