package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.{appResult, myAggregate, myAppSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * Desc: APP市场推广统计 <br/>
 *
 * 需求: 分渠道统计,每隔一秒钟,统计一个小时内,每个聚合,每种行为的用户数量 <br/>
 *
 * 随着智能手机的普及，在如今的电商网站中已经有越来越多的用户来自移动端，
 * 相比起传统浏览器的登录方式，手机APP成为了更多用户访问电商网站的首选。
 * 对于电商企业来说，一般会通过各种不同的渠道对自己的APP进行市场推广，
 * 而这些渠道的统计数据（比如，不同网站上广告链接的点击量、APP下载量）就成了市场营销的重要商业指标。
 *
 * create by shangbaishuyao on 2021/3/21
 *
 * @Author: 上白书妖
 * @Date: 20:09 2021/3/21
 */
object AppPromoteByChannel {
  def main(args: Array[String]): Unit = {
    //初始化环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //导入隐士转换
    import org.apache.flink.api.scala._
    //指定时间语义为eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //由于没有测试数据,需要自己生成随机的测试数据,即自定义source
    env.addSource(new myAppSource(10000))
      .assignAscendingTimestamps(appMarket1=>appMarket1.eventTime) //因为这里本身就是用到了精确到毫秒的时间了,所以我们不用乘以1000
      //根据渠道和行为分组,如果不考虑卸载的用户行为,我们需要做过滤
      .filter(appMarket1=> (!appMarket1.behavior.equals("UNINSTALL")))
      .keyBy(appMarket1 =>(appMarket1.channel,appMarket1.behavior)) //键
      .timeWindow(Time.hours(1),Time.seconds(1))
    //只要一开窗就立马想到两种选择.是使用增量的,还是使用全量的.
    //增量的话,选择性不大,用reduce也是可以的.但是用reduce写代码写的很多. 因为reduce默认情况下做增量聚合的时候他累加的时候,输出结果也必然是原来的类型
    //reduce是输入什么类型.输出也是什么类型.但是这里我们的业务需求是要输出另外一个样例类.reduce不是做不到.但是繁琐
    //一般来说,比较大小.在增量聚合中做过滤.在增量聚合中做条件判断用reduce比较合适. 如果你是做计数的话最好使用aggreagate.
    //因为aggregate给你自带了搞了一个计数器.对于只要做计数类的,我们使用aggregate.因为他有计数器.
      .aggregate(new myAggregate(),new appResult())
      /*
      AppMarketResult(2021-03-21 09:01:24--2021-03-21 10:01:24,xiaomiStore,INSTALL,19)
      AppMarketResult(2021-03-21 09:01:24--2021-03-21 10:01:24,头条,SEARCH,9)
      AppMarketResult(2021-03-21 09:01:24--2021-03-21 10:01:24,xiaomiStore,DOWNLOAD,24)
      AppMarketResult(2021-03-21 09:01:25--2021-03-21 10:01:25,AppStore,SEARCH,28)
      AppMarketResult(2021-03-21 09:01:25--2021-03-21 10:01:25,AppStore,CLICK,15)
       */
      .print()

    env.execute()
  }
}


//APP市场推广的样例类
case class appMarket(
                      userId:String,
                      behavior:String,
                      channel:String,
                      eventTime:Long
                    )



