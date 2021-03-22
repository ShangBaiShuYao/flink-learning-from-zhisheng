package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.{ResultAppClick, myAggregateAppClick}
import com.shangbaishuyao.bean.adClick
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * Desc: 页面广告点击量统计 <br/>
 *
 * 需求: 页面广告点击量统计,进行页面广告按照省份划分的点击量的统计 <br/>
 *  按照省份分组,每隔5秒钟,统计一个小时内的广告点击量
 *
 * 电商网站的市场营销商业指标中，除了自身的APP推广，
 * 还会考虑到页面上的广告投放（包括自己经营的产品和其它网站的广告）。
 * 所以广告相关的统计分析，也是市场营销的重要指标。
 * 对于广告的统计，最简单也最重要的就是页面广告的点击量，
 * 网站往往需要根据广告点击量来制定定价策略和调整推广方式，
 * 而且也可以借此收集用户的偏好信息。更加具体的应用是，
 * 我们的广告可以根据用户的地理位置进行划分，
 * 从而总结出不同省份用户对不同广告的偏好，这样更有助于广告的精准投放。
 * create by shangbaishuyao on 2021/3/21
 *
 * @Author: 上白书妖
 * @Date: 23:52 2021/3/21
 */
object AppAdClick {
  def main(args: Array[String]): Unit = {
    //初始化环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //导入隐士转换
    import org.apache.flink.api.scala._

    //配置数据源
    val stream1: DataStream[adClick] = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\AdClickLog.csv")
      .map(line => {
        val arr: Array[String] = line.split(",")
        val addClick = adClick(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim, arr(3).trim, arr(0).trim.toLong)
        addClick
      }).assignAscendingTimestamps(_.ClickTime * 1000) //因为日志时间是精确到秒,所以我们乘以1000精确到毫秒

    //统计每个省的用户点击数量. 不用做过滤,但是要做分组(一条数据代表点击了一次)
    val stream2: KeyedStream[adClick, String] = stream1.keyBy(addClick => {
      //按照每个省份分组
      addClick.province
    })
    //分完组之后开窗
    val steam3: WindowedStream[adClick, String, TimeWindow] = stream2.timeWindow(Time.hours(1), Time.seconds(5))

    //开完窗之后考虑使用增量聚合还是全量聚合. 这里是统计点击数量,我们使用增量聚合,因为它里面有累加器
    //又做累加又做增量聚合的话,一定使用aggregate
    steam3.aggregate(new myAggregateAppClick(),new ResultAppClick())
      .print()

    //执行
    env.execute()
  }
}
