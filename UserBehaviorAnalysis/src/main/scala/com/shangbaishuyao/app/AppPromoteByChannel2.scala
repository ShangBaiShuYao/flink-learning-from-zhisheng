package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.{myAppSource, myProcessAllWindowFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * Desc: APP市场推广统计 <br/>
 *
 * 需求: 不分渠道统计,每隔5秒钟,统计一个小时内,APP市场推广的用户数量 <br/>
 *
 * 可以不分组(keyBy). 输入数据转换成1. 因为每一条数据都代表一个用户. 用户id是uuid搞的. 虽然数据不可能重复,但是我们也考虑用户去重.
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
object AppPromoteByChannel2 {
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
      .assignAscendingTimestamps(appMarket2=>appMarket2.eventTime) //因为这里本身就是用到了精确到毫秒的时间了,所以我们不用乘以1000
      //根据渠道和行为分组,如果不考虑卸载的用户行为,我们需要做过滤
      .filter(appMarket2=>(!appMarket2.behavior.equals("UNINSTALL")))
      .map(_.userId)
      //数据没有分组,只能调用TimeWindowAll
      .timeWindowAll(Time.hours(1),Time.seconds(5))
      //如果要考虑去重只能使用全量比较合适.全量情况下用户量多使用bloom过滤器.如果用户量少,使用set去重就好了
      //由于你要去重.所以你最好使用全量聚合
      //增量不好去重的原因是. 你来一条数据就处理一查下.这样的话.你的用户id保存在哪里呢? 如果要增量去重的话,可以借助redis
      //做全量你要不就是使用process,要不就是apply.只不过process能做的事情更多. 建议使用process
      .process(new myProcessAllWindowFunction())
      .print()

    env.execute()
  }
}



/*

/**
 * @param maxElement 最多生成的数据条数
 */
class myAppSource(maxElement: Long) extends SourceFunction[appMarket]{
  //定义停止的条件, 就是什么时候数据不要再生成了
  var running : Boolean =true
  //定义渠道的数组
  var channelList = List("AppStore","huaweiStore","头条","xiaomiStore")
  //定义用户行为数组
  var behaviorList = List("CLICK","SEARCH","DOWNLOAD","INSTALL","UNINSTALL")
  //定义随机对象
  var r = new Random()

  //TODO 生成数据
  override def run(ctx: SourceFunction.SourceContext[appMarket]): Unit = {
    //需要累加器记录数据生成的条数
    var  count:Long = 0
    while (running && count<= maxElement){
      var userId :String = UUID.randomUUID().toString
      val market = new appMarket(userId, behaviorList(r.nextInt(behaviorList.size)), channelList(r.nextInt(channelList.size)), System.currentTimeMillis())
      count += 1 //累加计数器
      ctx.collect(market)
      Thread.sleep(20) //每生成一条数据等待一下
    }
  }
  //TODO 设置停止
  override def cancel(): Unit = {

  }
}


//APP市场推广按照渠道分组分析的结果
case class AppMarketResult(
                          WindowRange:String,
                          channel:String,
                          behavior:String,
                          userCount:Long
                          )

//APP市场推广的样例类
case class appMarket(
                    userId:String,
                    behavior:String,
                    channel:String,
                    eventTime:Long
                    )*/
