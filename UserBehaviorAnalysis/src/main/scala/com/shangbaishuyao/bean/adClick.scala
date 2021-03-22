package com.shangbaishuyao.bean


/**
 * 广告点击统计模块的样例类 <br/>
 * @param userId
 * @param adId
 * @param province
 * @param city
 * @param ClickTime
 */
case class adClick(
                    userId:Long,
                    adId:Long,
                    province:String,
                    city:String,
                    ClickTime:Long   //日志中是精确到秒的
                  )

