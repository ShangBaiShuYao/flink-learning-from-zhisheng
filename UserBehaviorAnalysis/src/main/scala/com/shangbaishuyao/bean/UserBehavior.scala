package com.shangbaishuyao.bean

/**
 * 用户行为的样例类
 *
 * @param userId
 * @param itemId
 * @param categoryId
 * @param behavior
 * @param timestamp
 */
case class UserBehavior(
                         userId:Long,
                         itemId:Long,
                         categoryId:Int,
                         behavior:String,
                         timestamp:Long)
