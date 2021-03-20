package com.shangbaishuyao.Handler

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 * Desc: 只能是此窗口的key状态下的所有数据
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 19:36 2021/3/20
 */
//累加完成之后，把累加的结果给这个WindowFunction输出,将累加之后的和转化为二元组输出(商品ID，该商品的访问次数)
class HotItemWindowFunction extends WindowFunction[Long,(Long,Long),Long,TimeWindow]{
  //apply这个方法是在窗口没有关闭后执行还是在窗口关闭后执行呢.所以这个方法在窗口关闭一关闭就开始执行了.
  //而且这个方法拿数据是从状态中拿数据. 关键是这里他执行拿到一个键的数据
  //如下所示: 这key是固定的. 这个key连的是一个迭代器. 所以只能拿到这一个键下的数据
  //那既然如此的话,就无法在这里面做排序 , 以为只有一个键下的数据,我无法和别人进行比较
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[(Long, Long)]): Unit = {
    //累加之后,将结果以二元组形式输出
    var itemId:Long=key //商品的id
    val count: Long = input.iterator.next() //该商品的访问次数
    //对外输出
    out.collect((itemId,count))
  }
}
