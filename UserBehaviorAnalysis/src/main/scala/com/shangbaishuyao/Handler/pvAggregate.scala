package com.shangbaishuyao.Handler

import com.shangbaishuyao.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

//一.这个是预处理的函数
//写一个类继承增量聚合函数
class pvAggregate extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator+1
  override def getResult(accumulator: Long): Long = accumulator
  override def merge(a: Long, b: Long): Long = a+b
}