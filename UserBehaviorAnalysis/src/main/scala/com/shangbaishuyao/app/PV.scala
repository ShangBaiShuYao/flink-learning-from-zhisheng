package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.{pvAggregate, pvResultWindow}
import com.shangbaishuyao.bean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Desc: 网站总浏览量（PV）的统计 <br/>
 * 统计一个小时内一个网站的PV
 *
 * 量网站流量一个最简单的指标，就是网站的页面浏览量（Page View，PV）。
 * 用户每次打开一个页面便记录1次PV，多次打开同一页面则浏览量累计。
 * 一般来说，PV与来访者的数量成正比，
 * 但是PV并不直接决定页面的真实来访者数量，
 * 如同一个来访者通过不断的刷新页面，也可以制造出非常高的PV。
 *
 * create by shangbaishuyao on 2021/3/21
 *
 * @Author: 上白书妖
 * @Date: 12:52 2021/3/21
 */
object PV {
  def main(args: Array[String]): Unit = {
    //初始化环境变量
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //指定时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.streaming.api.scala._
    //读取数据 , 并且设置时间语义和waterMark(水位线)
    //    env.readTextFile(getClass.getResource("").getPath)
    //83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
    //83.149.9.216 - - 17/05/2015:10:05:43 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png
    val stream : DataStream[UserBehavior] = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\UserBehavior.csv")
      //处理进来的每一条数据,转化为样例类对象;转化完成之后设置时间语义和waterMark
      .map(line => {
        //按照空格切分
        val arr: Array[String] = line.split(",")
        //向对象中填充数据
        new UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
      }).assignAscendingTimestamps(_.timestamp*1000)//因为这个数据是升序的,我们不是用乱序的方式,直接使用assignAscendingTimestamps

    //timewindow.他加了一个all是什么意思呢?是一个全量的开窗函数.全数据的开窗函数.什么叫全数据的开窗函数呢?就是如果你的数据不是键值对的就调用他.
    // 只要有一条数据来了,我们就认为有一个访问了,就是一个pv了.但是我们需要做过滤
    stream.filter(userBehavior=>{
        userBehavior.behavior.equals("pv")
      })
      //对每一条数据进行开窗,对每一个小时数据点击量进行统计
      .timeWindowAll(Time.hours(1))
    //开往窗之后就是处理问题了,到底是使用增量函数还是全量函数呢?
    //增量聚合函数: 来一条数据,立马计算,不等待. 全窗口函数:先将窗口所有数据收集起来,等到计算的时候遍历所有数据;因为统计pv的话,增量或者全量函数都是可以的
    //增量聚合函数有reduceFunction,AggregateFunction
    //全窗口函数: ProcessWindowFunction, ProcessAllWindowFunction
    //reducess写起来没有aggregate写起来那么方便. 因为reduce要做预处理的话.他出入的对象和传出的对象是相同的. 其实我们没有必要,因为我们统计个数,直接输出一个数字就可以了
      .aggregate(new pvAggregate(),new pvResultWindow())
      .print()

    //执行
    env.execute()
  }
}

