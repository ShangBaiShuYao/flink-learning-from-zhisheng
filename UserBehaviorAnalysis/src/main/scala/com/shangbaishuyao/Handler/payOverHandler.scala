package com.shangbaishuyao.Handler

import java.util

import com.shangbaishuyao.bean.{OrderEvent, OrderMsg}
import org.apache.flink.cep.PatternSelectFunction

//订单在15分钟之内支付成功数据处理. 这里既可以得到start,也可以得到follow
class payOverHandler extends PatternSelectFunction[OrderEvent,OrderMsg]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderMsg = {
    val createOrder: OrderEvent = map.get("begin").iterator().next()
    val payOrder: OrderEvent = map.get("follow").iterator().next()
    OrderMsg(createOrder.orderId,"订单支付正常",createOrder.orderTime,payOrder.orderTime)
  }
}
