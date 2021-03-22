package com.shangbaishuyao.Handler

import java.text.SimpleDateFormat
import java.util.Date
import com.shangbaishuyao.bean.adClickResult
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//窗口触发的时候执行的返回结果方法
//key: 我们是按照省份分组的
class ResultAppClick extends WindowFunction[Long,adClickResult,String,TimeWindow]{
  //这个迭代器中是一条数据.他是增量聚合.且前面做个一个累加器的累加
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[adClickResult]): Unit = {
    lazy val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val windowStart: Long = window.getStart
    val windowEnd: Long = window.getEnd
    val windowRange: String = format.format(new Date(windowStart)) + "--" + format.format(new Date(windowEnd))
    out.collect(new adClickResult(windowRange,key,input.last))
  }
}
