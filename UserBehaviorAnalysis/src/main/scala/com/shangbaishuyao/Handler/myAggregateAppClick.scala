package com.shangbaishuyao.Handler

import com.shangbaishuyao.bean.adClick
import org.apache.flink.api.common.functions.AggregateFunction
/**
 * Desc:
 *  点击量的累加器
 *  增量聚合做累加
 * create by shangbaishuyao on 2021/3/22
 * @Author: 上白书妖
 * @Date: 5:33 2021/3/22
 */
class myAggregateAppClick extends AggregateFunction[adClick,Long,Long]{
  override def createAccumulator(): Long = 0

  override def add(value: adClick, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  //这个合并过程在什么时候能调用的呢?
  //事实上这个和sparkRDD的过程类似.
  //因为数据像流一样流过来. 我们可能会有很多个流.因为你有并行度
  //假设我有两个流. 他开窗是在什么时候开窗呢? 开窗只是个逻辑.他不是真正的处理.他只是将数据分成一个其实时间和一个结束时间.
  //真正处理是开完窗之后的聚合. 开完窗之后的聚合就是AggregateFunction.
  //你数据来了之后,我首先给你加上一个时间标签.通过时间标签标记你属于那个window. 因为开窗只是给你加了一个其实时间和结束时间.
  //每条数据我给你归了一个范围.并不是真正做处理. 做处理是开窗之后的聚合函数.实际上给这些数据加了一个时间标签之后.
  //接下来开始要做聚合函数了. 为什么要做聚合函数. 因为我们使用的是增量聚合函数. 只要来了一条数据属于我这个窗口的.我就开始做增量聚合
  //做增量聚合,假如你有两个分区,说白了就是两个并行度.首先对其中一个分区做累加器加1,(a,1)(b,1).第二个分区也是一样做增量聚合(a,5)(b,1). 加下来,这个keyBy这个
  //地方是shuffle算子. shuffle算子这个地方需要落地磁盘. 而Flink里面是直接从内存中读的.而我们的两个分区的数据,如(a,1)和(a,5)最后是要分到
  //一组里面去的.那么这两个shuffle过程后的(a,1)和(a,5)聚合使用add(value: adClick, accumulator: Long)吗?不会,因为(a,1)(a,5)不能算是
  //一条数据,准确的说,(a,1)(a,5)是聚合之后的结果.他只是来自两个不同的分区. 来自两个不同分区的聚合之后的累加器的话.那我就是a累加器加上b累加器
  //merge就是将相同k的两个累加器相加. 这个和spark的RDD一样的.
  override def merge(a: Long, b: Long): Long = a+b
}
