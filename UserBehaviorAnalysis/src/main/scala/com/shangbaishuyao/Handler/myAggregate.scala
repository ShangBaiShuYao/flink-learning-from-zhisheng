package com.shangbaishuyao.Handler

import com.shangbaishuyao.app.appMarket
import org.apache.flink.api.common.functions.AggregateFunction
/**
 * Desc:
 * create by shangbaishuyao on 2021/3/22
 * @Author: 上白书妖
 * @Date: 5:34 2021/3/22
 */
class myAggregate() extends AggregateFunction[appMarket,Long,Long]{
  override def createAccumulator(): Long = 0

  override def add(value: appMarket, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}