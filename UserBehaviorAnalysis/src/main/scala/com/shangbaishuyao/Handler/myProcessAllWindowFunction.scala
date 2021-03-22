package com.shangbaishuyao.Handler

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable

/**
 * Desc:
 *
 * 输入类型看开窗前面是什么.
 * 最后输出每一个窗口统计了多少个用户.
 *
 *
 * create by shangbaishuyao on 2021/3/22
 * @Author: 上白书妖
 * @Date: 5:49 2021/3/22
 */
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