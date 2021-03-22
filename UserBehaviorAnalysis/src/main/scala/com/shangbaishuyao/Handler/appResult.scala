package com.shangbaishuyao.Handler

import java.sql.Date
import java.text.SimpleDateFormat
import com.shangbaishuyao.bean.AppMarketResult
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Desc: 触发窗口函数 <br/>
 * create by shangbaishuyao on 2021/3/22
 * @Author: 上白书妖
 * @Date: 5:38 2021/3/22
 */
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
