package com.shangbaishuyao.Handler

import java.util
import com.shangbaishuyao.bean.{OrderEvent, OrderMsg}
import org.apache.flink.cep.PatternTimeoutFunction

//对订单支付超时的处理
//当在15分钟内出现支付超时的数据的处理,只能找到这个start即创建订单的事件. 找不到follow,即支付的事件.
//所以map.get("follow")这是不可能找到的. 这表示从数据中找支付完的数据,肯定找不到. 你只能找start的数据.
class payOutTimeHandler extends PatternTimeoutFunction[OrderEvent,OrderMsg]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderMsg = {
    //得到一个订单的数据事件
    val order: OrderEvent = map.get("begin").iterator().next()
    //返回                                                                          //创建订单的时间
    OrderMsg(order.orderId, "该订单在15分钟之内没有支付", order.orderTime, 0)
  }
}
