package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.MyTrigger
import com.shangbaishuyao.bean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Desc:
 * 就是监控用户的上网异常行为
 * 类似某个时间点
 * 流量异常大
 * 或者其他别的一些行为
 * 但是这个网络异常检测基本上就是基于流量的检测才能体现出来
 * 或者说这个用户某个时间段流量一直为空
 * 可能是断网了或者什么其他情况
 * 就是说这个用户正常流量是一个区间值
 * 然后流量为空了
 * 或者达到某个阈值会有提醒
 * 为空的时候肯定是保持多久
 * 比如五分钟十分钟这样子
 * 在后台就显示异常
 * 用户可能断网什么的
 * 流量过大就是显示该用户流量过大，异常下载之类

 * 就是不是单纯的监控流量
 * 把流量监控分为三个区间
 * 空 是异常   过大是异常
 * 还有一些
 * 正常行为
 * 出现异常的时间提醒
 * create by shangbaishuyao on 2021/3/21
 *
 * @Author: 上白书妖
 * @Date: 14:10 2021/3/21
 */
object GA783 {
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
        UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000) //因为这个数据是升序的,我们不是用乱序的方式,直接使用assignAscendingTimestamps

//    stream.filter(userBehavior => {
//      userBehavior.timestamp
//    })

    //执行
    env.execute()
  }
}

