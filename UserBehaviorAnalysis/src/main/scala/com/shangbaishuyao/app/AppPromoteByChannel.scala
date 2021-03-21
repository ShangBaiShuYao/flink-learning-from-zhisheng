package com.shangbaishuyao.app

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.util.Random

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
      .assignAscendingTimestamps(appMarket=>appMarket.eventTime) //因为这里本身就是用到了精确到毫秒的时间了,所以我们不用乘以1000
      //根据渠道和行为分组,如果不考虑卸载的用户行为,我们需要做过滤
      .filter(appMarket=>(!appMarket.behavior.equals("UNINSTALL")))
      .keyBy(appMarket =>(appMarket.channel,appMarket.behavior)) //键
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

class appResult() extends WindowFunction[Long,AppMarketResult,(String,String),TimeWindow]{
  //当前上下文里面的input里面只有一条数据.因为这个appResult类里面的apply函数,在窗口触发的时候执行的
  //它符合你窗口触发条件才会触发一次的. 这个时候触发有什么用呢? 我前面数据都在我预处理中累加完了.累加完了当然只有一条了
  //我们不是有很多组吗? 是的, 但是在前面我们已经分好组了. 这个方法里面只有其中一组的数据.
  //既然是其中一组的数据的话, 那么其迭代器中只有一条数据. 这一条里面就是包含我用户的数量.
  override def apply(key: (String, String), window: TimeWindow, input: Iterable[Long], out: Collector[AppMarketResult]): Unit = {
    lazy val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    var windowEnd = window.getEnd //long类型的
    var windowStart = window.getStart
    val windowRange: String = format.format(new Date(windowStart)) + "--" + format.format(new Date(windowEnd))
    //得到用户数量
    val userCount: Long = input.last //既然只有一条,我直接last最后一条
    out.collect(AppMarketResult(windowRange,key._1,key._2,userCount))
  }
}



class myAggregate() extends AggregateFunction[appMarket,Long,Long]{
  override def createAccumulator(): Long = 0

  override def add(value: appMarket, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}


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
                    )