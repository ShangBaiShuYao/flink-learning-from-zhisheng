package com.shangbaishuyao.Handler

import java.sql.Date
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Desc: 全窗口聚合排序类 <br/>
 * 这个是没有key的,是你全窗口的数据
 * create by shangbaishuyao on 2021/3/20
 *
 * @Author: 上白书妖
 * @Date: 18:31 2021/3/20
 */
class HotItemAllWindowFunction(topN:Int) extends ProcessAllWindowFunction[(Long,Long),String,TimeWindow]{
  override def process(context: Context, elements: Iterable[(Long, Long)], out: Collector[String]): Unit = {
    //从Iterable中得到数据 , 按照访问次数降序排序 _.2这是升序,如果要降序必须接上取反
    val tuples: List[(Long, Long)] = elements.toList.sortBy(_._2)(Ordering.Long.reverse).take(topN)
    val stringBuilder: StringBuilder = new StringBuilder
    stringBuilder.append(" 当前一个小时内的数据前 "+topN +"个  窗口时间："+ new Date(context.window.getEnd) +" \n")
    tuples.foreach(tuples =>{
      stringBuilder.append("商品ID："+tuples._1+",访问的次数是: "+tuples._2+"\n")
    })
    stringBuilder.append("----------------------------------\n")
    out.collect(stringBuilder.toString())
  }
}
