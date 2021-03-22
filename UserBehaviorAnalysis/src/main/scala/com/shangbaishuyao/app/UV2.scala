package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.{MyTrigger, UVByBoomResultProcess}
import com.shangbaishuyao.bean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Desc: 网站独立访客数（UV）的统计 <br/>
 *
 * 统计UV就是统计一个小时内,整个网站的UV
 * 分析: 实际上只需要用到用户id就可以了.其他数据根本用不到
 *
 * create by shangbaishuyao on 2021/3/21
 *
 * @Author: 上白书妖
 * @Date: 14:10 2021/3/21
 */
object UV2 {
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
    val stream: DataStream[UserBehavior] = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\UserBehavior.csv")
      //处理进来的每一条数据,转化为样例类对象;转化完成之后设置时间语义和waterMark
      .map(line => {
        //按照空格切分
        val arr: Array[String] = line.split(",")
        //向对象中填充数据
        new UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000) //因为这个数据是升序的,我们不是用乱序的方式,直接使用assignAscendingTimestamps
    //统计UV
    //timewindow.他加了一个all是什么意思呢?是一个全量的开窗函数.全数据的开窗函数.什么叫全数据的开窗函数呢?就是如果你的数据不是键值对的就调用他.
    // 只要有一条数据来了,我们就认为有一个访问了,就是一个pv了.但是我们需要做过滤
    stream.filter(userBehavior => {
      userBehavior.behavior.equals("pv")
    }).map(userBehavior=>{
      ("uv",userBehavior.userId)
    }).keyBy((_,1))
        .timeWindow(Time.hours(1))
      //自定义新的窗口触发机制
        .trigger(new MyTrigger())
//        .process(new UVByBoomResultProcess())
//        .print("main")
    //执行
    env.execute()
  }
}

