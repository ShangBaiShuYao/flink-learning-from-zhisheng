package com.shangbaishuyao.bean

/**
 * 订单事件样例类 <br/>
 * @param orderId
 * @param state
 * @param payId
 * @param orderTime
 */
case class OrderEvent(
                       orderId: Long,
                       state: String,
                       payId: String,
                       orderTime: Long)