package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.{ResultAppClick, filterProcessFunction, myAggregateAppClick}
import com.shangbaishuyao.bean.adClick
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * Desc: 页面广告点击量统计 <br/>
 *
 * 需求: 页面广告点击量统计,进行页面广告按照省份划分的点击量的统计 <br/>
 *  按照省份分组,每隔5秒钟,统计一个小时内的广告点击量
 *
 *  设置一个阈值(同一个用户对同一个广告在一个小时内最多只能点击N次),即这个用户在这个阈值内点击多少次都是正常的.
 *  一旦超过就认为这个用户在刷单.或者在刷流量.那么这个用户需要过滤掉. 用户过滤的黑名单需要一个时间有效期
 *  <p> 使用状态编程来完成过滤黑名单的问题,防止用户刷流量的行为 <p/>
 *
 *
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
object AppAdClick2 {
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

    //定义输出标签 就输出输入进来的类型. 我们当前类型不动 ("黑名单，即统一用户点击同一广告次数超过阈值的侧输出流标签")
    val outPutTag: OutputTag[adClick] = new OutputTag[adClick]("filter_user")
    //TODO 过滤哪些黑名单的用户,防止用户刷流量的行为. 由于底层API. 即processFunction API他能做到输入一条, 输出0,或者N条
    //重点是底层API可以输出0条. 如果你不符合我们的条件.我就将当前用户过滤掉. 当前用户过滤掉不就是输出0么. filter算子他做不到.
    // 因为业务非常复杂,filter算子他是做不到的,因为他需要统计用户的访问次数的. 你在Filter里面是不好统计的.所以我只能在底层的API中
    //去统计. 第二点就是,刚好这个底层API.即processFunction API也可以帮我们做过滤. 为什么能帮我做过滤呢?因为他可以做到,来一条数据,我可以输出0
    //来一条数据输出0不就是不输出嘛. 那就是过滤了. 这两个理由可以判断出processFunctionAPI绝对可以帮我们做过滤
    //步骤: 1.先根据用户id个广告id分组.(就是考虑,这个用户频繁点击这个广告这才叫刷单,而不是这个用户点击了这个广告点击那个广告.这不叫刷单)
    //     2.统计用户的点击数量(累加)
    //     3.根据累加之后的结果和阈值进行比对,来确定是否过滤,即确定黑名单.如归确定了黑名单我就讲黑名单放到侧输出流中. 主流不输出了.即输出0就是不输出的意思
    val keyedStream: KeyedStream[adClick, (Long, Long)] = stream1.keyBy(addClick => {
      (addClick.userId, addClick.adId)
    })
    //过滤要执行底层函数.直接执行process,只有使用process才有可能用到底层函数.传一个阈值进去.你的阈值到达多少才才过滤
    //TODO 过滤操作输入的是addClick,输出也是addClick.这样就保证我后面的开窗聚合就不用了改代码了
    val sideSteam: DataStream[adClick] = keyedStream.process(new filterProcessFunction(6, outPutTag))
    //打印侧输出流
    sideSteam.getSideOutput(outPutTag).print("sideOutput==>")

    //统计每个省的用户点击数量. 不用做过滤,但是要做分组(一条数据代表点击了一次)
    val stream2: KeyedStream[adClick, String] = keyedStream.keyBy(addClick => {
      //按照每个省份分组
      addClick.province
    })

    //分完组之后开窗
    val steam3: WindowedStream[adClick, String, TimeWindow] = stream2.timeWindow(Time.hours(1), Time.seconds(5))

    //开完窗之后考虑使用增量聚合还是全量聚合. 这里是统计点击数量,我们使用增量聚合,因为它里面有累加器
    //又做累加又做增量聚合的话,一定使用aggregate
    steam3.aggregate(new myAggregateAppClick(),new ResultAppClick())
      .print("mainOutPut-->")

    //执行
    env.execute()
  }
}
