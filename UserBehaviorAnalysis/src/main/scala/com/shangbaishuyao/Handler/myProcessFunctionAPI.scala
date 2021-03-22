package com.shangbaishuyao.Handler

import com.shangbaishuyao.bean.{OrderEvent, OrderMsg}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

//当前的业务场景: 事件的时间是升序的,而且一个订单不会创建两次. 在大数据中为什么要做数据清洗呢? 就是把不正常的数据给过滤掉.所以按道理说不应该会出现数据不对的情况
//即不会出现同一个订单出现两次, 也不会出现同一个订单支付两次.
//一开始订单创建的话,我不能百分百保证他是保存成功了.所以我这个状态保存订单创建的哪些数据.
class myProcessFunctionAPI extends KeyedProcessFunction[Long,OrderEvent,OrderMsg]{
  //这时候就需要想清楚,到底用状态保存什么东西
  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderMsg]#Context, out: Collector[OrderMsg]): Unit = {

  }
}
