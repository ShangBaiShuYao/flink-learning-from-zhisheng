package com.shangbaishuyao.app

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.shangbaishuyao.Handler.{CountAggregate, HotItemAllWindowFunction, HotItemWindowFunction}
import com.shangbaishuyao.bean.{GA783_Bean, UserBehavior}
import com.shangbaishuyao.utils.{GA783_ProducerKafka, MyKafkaUtil}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Desc: 实时热门商品统计<br/>
 *
 * 其实就是用来统计我们所谓的热门的TopN的商品,
 * 不过呢他有一个要求:统计最近一个小时内的TopN.
 * 他有一个间隔,比如他有一分钟一次或者5分钟一次这么一个时间间隔.
 * 累加商品的访问次数取TopN.
 *
 * create by shangbaishuyao on 2021/4/1
 * @Author: 上白书妖
 * @Date: 10:05 2021/4/1
 */
object GA783 {
  def main(args: Array[String]): Unit = {
      //1、初始化环境
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      import org.apache.flink.streaming.api.scala._
      //指定时间语义为事件时间
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      //2、读取kafka数据
      val props = new Properties()
      props.setProperty("bootstrap.servers",GA783_ProducerKafka.bootstraps)
      props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
      props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
      props.setProperty("group.id","consumer-group1")
      props.setProperty("auto.offset.reset","latest")
      val stream: DataStream[UserBehavior] = env.addSource(new FlinkKafkaConsumer[String](GA783_ProducerKafka.userBehaviorTopic, new SimpleStringSchema(), props).setStartFromEarliest())
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
    var sb :StringBuilder =new StringBuilder()

    stream2.timeWindowAll(Time.minutes(5))
      .process(new HotItemAllWindowFunction(3,sb))



      //写入mysql数据库
//      stream2.addSink(new MyJdbcSink(sb))

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

  /**
  当前一个小时内的数据前 3个  窗口时间：Sun Nov 26 11:30:00 CST 2017
    商品ID：2338453,访问的次数是: 28
    商品ID：812879,访问的次数是: 17
    商品ID：2364679,访问的次数是: 15
   */
  //全窗口聚合排序类
  class HotItemAllWindowFunction(topN:Int,sb :StringBuilder) extends ProcessAllWindowFunction[(Long,Long),String,TimeWindow]{
    var ga783_bean:GA783_Bean = _
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    //open 主要是创建连接
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)

      conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/ga783", "root", "shangbaishuyao")
//      insertStmt = conn.prepareStatement("INSERT INTO user (pid, number) VALUES (?, ?)")
//      updateStmt = conn.prepareStatement("UPDATE user SET number = ? WHERE pid = ?")
      insertStmt = conn.prepareStatement("INSERT INTO person (pid, number) VALUES (?, ?)")
      updateStmt = conn.prepareStatement("UPDATE person SET number = ? WHERE pid = ?")
    }
    override def process(context: Context, elements: Iterable[(Long, Long)], out: Collector[String]): Unit = {
      lazy val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      //从Iterable 得到数据按照访问的次数降序排序
      val tuples: List[(Long, Long)] = elements.toList.sortBy(_._2)(Ordering.Long.reverse).take(topN)
      //      sb.append(" 当前一个小时内的数据前 "+topN +"个  窗口时间："+format.format(new Date(context.window.getEnd)) +" \n")
      tuples.foreach(t=>{
//        Thread.sleep(100)
        //        sb.append("商品ID："+t._1+",访问的次数是: "+t._2+"\n")
        ga783_bean = GA783_Bean(t._1.toString,t._2.toInt)
        println("商品ID："+ga783_bean.pid+",访问的次数是: "+ga783_bean.number+"\n")
        updateStmt.setString(1, ga783_bean.pid)
        updateStmt.setInt(2, ga783_bean.number)
        updateStmt.execute()
        if (updateStmt.getUpdateCount == 0) {
          insertStmt.setString(1, ga783_bean.pid)
          insertStmt.setInt(2, ga783_bean.number)
          insertStmt.execute()
        }
      })
//      sb.append("----------------------------------\n")
      out.collect(sb.toString())
    }
    override def close(): Unit = {
      insertStmt.close()
      updateStmt.close()
      conn.close()
    }

  }
}
