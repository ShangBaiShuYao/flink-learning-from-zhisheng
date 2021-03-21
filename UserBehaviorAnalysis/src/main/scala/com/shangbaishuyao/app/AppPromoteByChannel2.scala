package com.shangbaishuyao.app

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.Random

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
      .assignAscendingTimestamps(appMarket=>appMarket.eventTime) //因为这里本身就是用到了精确到毫秒的时间了,所以我们不用乘以1000
      //根据渠道和行为分组,如果不考虑卸载的用户行为,我们需要做过滤
      .filter(appMarket=>(!appMarket.behavior.equals("UNINSTALL")))
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
//输入类型看开窗前面是什么.
//最后输出每一个窗口统计了多少个用户.
class myProcessAllWindowFunction extends ProcessAllWindowFunction[String,(String,Long) ,TimeWindow]{
  //定义set集合去重 .set一定要是可变类型mutable.因为要往里面放数据. [String]用户id类型
  //set集合的初始化放到open方法中去重最好
  var userSet: mutable.Set[String] = _

  //open()方法是rich function的初始化方法，当一个算子例如map或者filter被调用之前open()会被调用。
  //初始化Set集合做去重
  override def open(parameters: Configuration): Unit = {
    userSet = mutable.Set[String]()
  }

  //process在窗口触发的时候执行. 不一定是在窗口结束的时候. 因为搞不好你有水位线.或者延迟触发
  override def process(context: Context, elements: Iterable[String], out: Collector[(String, Long)]): Unit = {
    lazy val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val windowStart: Long = context.window.getStart
    val windowEnd: Long = context.window.getEnd
    val windowRange: String = format.format(new Date(windowStart)) + "--" + format.format(new Date(windowEnd))
    for (element <- elements){
      userSet.add(element)
    }
    //这个set集合的长度就是用户的数量
    out.collect((windowRange,userSet.size))
    //这个地方我不用set集合,我放在状态中可不可以呢? 可以. 当然,在状态中我们不可以用set集合了.
    //我们状态中我们常用的有三种:值状态. 列表状态. map状态 所以我们使用map状态帮我做去重.
    //map里面不但可以帮我们做去重,还可以告诉我们,哪一个用户有多少个也可以算的.
    //那我们并行度比较高怎么办呢? 并行度比较高做去重的话,只能使用redis了.redis里面保存用户的数据,每次操作的时候拿出来再加1
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
