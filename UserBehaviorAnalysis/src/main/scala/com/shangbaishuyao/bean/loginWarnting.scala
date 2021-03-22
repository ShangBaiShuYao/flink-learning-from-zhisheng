package com.shangbaishuyao.bean

/**
 * 恶意登录 用户告警信息
 * @param userId
 * @param firstTime
 * @param secondTime
 */
case class loginWarnting(
                        userId:Long,
                        firstTime:Long,
                        secondTime:Long
                        )
