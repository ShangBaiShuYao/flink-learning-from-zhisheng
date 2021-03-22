package com.shangbaishuyao.bean

/**
 * 订单监控的提示信息<br/>
 * @param orderId
 * @param msg
 * @param createTime
 * @param payTime
 */
case class OrderMsg(
                     orderId: Long,
                     msg: String,
                     createTime: Long,
                     payTime: Long)
