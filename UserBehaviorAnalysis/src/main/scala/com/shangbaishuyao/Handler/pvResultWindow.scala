package com.shangbaishuyao.Handler

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//二.这个是触发的函数
//输入类型是Long类型. 输出类型:我们最后输出的是某一个小时时间内他的pv的个数,所以有两个东西,一个是时间,一个是个数,所以定义一个二元组
//时间范围我们使用字符串,pv的个数使用Long类型的. 因为我们没有key,所以这里就不能继承他了
//class pvResultWindow extends WindowFunction[Long,(String,Long),,TimeWindow]{}
//所以我们继承AllWindowFunction. 他就不用key了, 而是将所有数据拿出来
class pvResultWindow extends AllWindowFunction[Long,(String,Long),TimeWindow]{
  lazy val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[(String, Long)]): Unit = {
    //input:这个input代表所有的.既然是所有的,我能只需要拿到最后一个就可以了. 因为我们没有分组,窗口里面没有分组.他返回的最后一个就是的
    //虽然是迭代器.我们只需要拿他最后一个就可以了
    //时间窗口的开始和结束,即窗口的时间范围
    val windowRange: String = format.format(new Date(window.getStart)) + "--" + format.format(new Date(window.getEnd))//窗口的开始时间和窗口的结束时间
    //输出时间范围和迭代器最后一个值
    out.collect((windowRange,input.last))
  }
}
