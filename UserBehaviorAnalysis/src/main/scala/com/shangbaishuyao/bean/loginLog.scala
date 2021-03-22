package com.shangbaishuyao.bean

/**
 * 恶意登录监控的样例类 <br/>
 * @param userId
 * @param ip
 * @param loginType
 * @param loginTime
 */
case class loginLog(
                     userId:Long,
                     ip:String,
                     loginType:String,
                     loginTime:Long
                   )
