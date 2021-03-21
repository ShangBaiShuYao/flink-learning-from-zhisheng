package com.shangbaishuyao.app

import java.text.SimpleDateFormat
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable

/**
 * Desc: 每隔5秒，输出最近10分钟内访问量最多的前N个URL <br/>
 *
 * 需要保证数据绝对准确.(想要保证数据绝对正确,根绝数据特点,只能延迟一分钟.)
 * create by shangbaishuyao on 2021/3/20
 *
 * @Author: 上白书妖
 * @Date: 21:50 2021/3/20
 */
object topNPage {
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
//    val stream : DataStream[pageViewEvent] = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\apache.log")
    val stream : DataStream[pageViewEvent] = env.socketTextStream("hadoop102",7777)
      .map(line => {
      val arr: Array[String] = line.split(" ")
      val format: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      var eventTime: Long = format.parse(arr(3)).getTime //得到毫秒数
      new pageViewEvent(arr(0).trim, eventTime, arr(5).trim, arr(6).trim) //这种是调用乱序的情况下       //延迟两秒
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[pageViewEvent](Time.seconds(2)) {
      override def extractTimestamp(element: pageViewEvent) = element.eventTime
    })
    //分组,因为我们需要的是每隔5秒，输出最近10分钟内访问量最多的前N个URL
    //所以我们需要按照url分区,了解10分钟内,每个url的总次数
    val keyStream: KeyedStream[pageViewEvent, String] = stream.keyBy(_.url)
    //迟到的数据满足 watermark < windowEnd+allowedLateness  迟到时间+延迟时间 > 水位线 的话. 来一条迟到的数据就会触发一次
    //因为迟到的数据如果符合条件,你迟到数据来一条就会触发一次,来一条就会触发一次.所以对某一个窗口来说会有多条输出结果.所以为了保证数据的准确性,我们会拿最后一个数据
    //开窗: 窗口大小是10分钟,滑动步长是5秒钟. 为保证迟到的数据也能进入窗口我们需要设置AllowedLateness(Time)        //本质上就是:waterMark < (window-end+allowLateness)的数据进来都是会再次触发窗口计算
    val WindowedStream: WindowedStream[pageViewEvent, String, TimeWindow] = keyStream.timeWindow(Time.minutes(10), Time.seconds(5)).allowedLateness(Time.minutes(1))//允许迟到一分钟数据进入
    // preAggregator: AggregateFunction[T, ACC, V] 这个是预聚合,预处理,就是来一条数据就处理,就算一次,这叫预聚合
    // windowFunction: WindowFunction[V, R, K, W] 这个windowFunction就是我们触发窗口的一个函数
    //aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation] 最后输出的类型,你想要输出什么类型.
    //     ACC       V:是两个函数之间传递的一个东西 R:是什么意思? R是最后输出的,所以我们最后要考虑类型了. 做聚合的时候,你需要考虑最后输出类型
    //我们最后输出需要返回(url,count,window信息)  window信息包含窗口起始时间和结束时间. 由于包含多个值. 所以可以定义样例类
    //开完窗之后做增量聚合
    //我们生命类型是声明在这两个函数类中的.  第一个函数类做预聚合的或者说做增量聚合的 第二个函数类是最后输出结果的
    val stream2: DataStream[urlCountView] = WindowedStream.aggregate(new UrlCountAgg(), new UrlCountResultProcess())
      //测试打印结果查看是否符合条件
      //测试结果
      //urlCountView(1431828335000-1431828345000,/presentations/logstash-monitorama-2013/images/kibana-dashboard3.png,2,1431828345000)
      //urlCountView(1431828335000-1431828345000,/presentations/logstash-monitorama-2013/images/kibana-dashboard3.png,3,1431828345000)
//      .print()
    //根据每个窗口输出的数据(记住: 一个窗口输入统一个url即同一个key会输出n条数据),所以我们取同一个窗口输出多次数据的最后一次输出结果
    //即,取同一个key(url)的最后一条数据,然后按照Count降序排序

    //记住:这是流式处理,你只要窗口一输出,我立马做排序
    //所以我上面如果做了延迟处理下面就需考虑延迟情况了.你要使用覆盖,延迟一分钟即一分钟之后把前面输出的结果覆盖一下
    stream2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[urlCountView](Time.seconds(5)) {//设置延迟5秒
      override def extractTimestamp(element: urlCountView) = {
        element.windowEnd
      }
    })
        .keyBy(_.windowRange)//按照每个窗口来进行分组
        .timeWindow(Time.seconds(10))//分完组之后又开窗
        .apply(new HotUrlSortResult()) //用全量数据的聚合函数
        .print("第二次开窗处理结果:")

    //执行
    env.execute()
  }

  //全量聚合函数,即来一条数据,立马计算,不等待
  class HotUrlSortResult() extends WindowFunction[urlCountView,String,String,TimeWindow]{
    /**
     * @param key
     * @param window
     * @param input 整个一个键里面的所有数据都在里面
     * @param out
     */
    override def apply(key: String, window: TimeWindow, input: Iterable[urlCountView], out: Collector[String]): Unit = {
      //apply里面传了一个键,说明一个窗口里面的所有数据都来了.那这个窗口里面的每一个url的大小来排序
      //可变的集合  mutable:表示可变的集合
      val map: mutable.Map[String, urlCountView] with Object =  mutable.Map[String, urlCountView]() //存放每一个URL中最大的Count
      for (one <- input){
        //先判断map集合中是否存在值,如果存在还需要判断count的大小
        if (map.get(one.url)==None){//如果是None表示没有数据,没有数据则直接放进去
          map.put(one.url,one)
        }else{
          val preCount: Long = map.get(one.url).get.count
          if (preCount < one.count){
            //如果preCount小于当前的Count,我们做替换
            map.put(one.url,one)
          }
        }
      }
      //把每一个url的最大count按照降序排序
      val list: List[urlCountView] = map.values.toList.sortBy(_.count)(Ordering.Long.reverse).take(3)
      //输出
      val stringBuilder: StringBuilder = new mutable.StringBuilder()
      stringBuilder.append("当前统计的时间范围是:"+key).append("\n")
      list.foreach(urlCount=>{
        stringBuilder.append("url页面是:"+urlCount.url,"访问次数是:"+urlCount.count).append("\n")//换行操作
      })
      stringBuilder.append("---------------------------------------")
      out.collect(stringBuilder.toString())
    }
  }

  //统计每个页面的访问次数(这个是增量累加的)
  //AggregateFunction<IN, ACC, OUT> : 声明类型: IN是输入类型, ACC是做统计的,就是我们的累加器,我们这里只需要累加个数就可以, OUT是输出类型,输出的是累加之后的结果
  class UrlCountAgg() extends AggregateFunction[pageViewEvent,Long,Long]{
    //初始化这个累加器,一开始这个累加器一般都是等于0. 除非你有特殊需求
    override def createAccumulator(): Long = 0
    //这个表示你的累加器怎么加. 即你的累加规则是什么样的
    override def add(value: pageViewEvent, accumulator: Long): Long = accumulator + 1 //accumulator即累加器每次加1,就是说当前这个页面,你来了一次访问我这个累加器就加1
    //返回结果只需要返回累加器的值就可以了
    override def getResult(accumulator: Long): Long = accumulator
    //合并
    override def merge(a: Long, b: Long): Long = a+b
  }


  //WindowFunction[IN, OUT, KEY, W <: Window]: 这个IN是由增量累加之后传给他的,OUT输出,这里我们需要输出一个封装类型.key我们以什么作为键呢?我们以url作为键,这里url是String类型的 .窗口是什么类型的? 是timeWindow类型的
  //使用processWindowFunction也可以,因为ProcessWindowFunction他是最底层的API,只有前面的API做不到我们才使用它
  class UrlCountResultProcess() extends WindowFunction[Long,urlCountView,String,TimeWindow]{
    //这是一个窗口触发的函数,里面传入的数据是一组里面的数据,即一个键(key)里面的所有数据
    //这个键里面的所有数据实际上我们只要拿最后一条就可以了,因为他最后一次输出的数据才是当前页面的访问次数
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[urlCountView]): Unit = {
      val start: Long = window.getStart //窗口起始时间
      val end: Long = window.getEnd //窗口结束时间
      val range: String = start + "-" + end //窗口起始和结束时间范围
      //最后一条数据就是我们计算的结果
      //以为我们之前就是根据url分组的,所以url就是key
      val view: urlCountView = new urlCountView(range, key, input.last, end)
      //对外输出
      out.collect(view) //当然这里只是计算每个url的次数,还是没有做任何排序的. 因为这里不可能做排序的
      //这里面不能做排序,因为这里面只是一个键的所有数据. 即我们计算每个url的访问次数. 和这个key就是url
    }
  }

  //热门URL统计聚合的结果,样例类
  case class urlCountView(
                         windowRange:String, //起始时间和结束时间都放进去
                         url:String,         //地址
                         count:Long,         //计算结果
                         windowEnd:Long      //窗口结束时间,这个我们专门用来分组的字段
                         )

  //声明样例类,服务器访问日志
  case class pageViewEvent(
                          ip:String,
                          eventTime: Long,
                          method:String,
                          url:String
                          )
}
