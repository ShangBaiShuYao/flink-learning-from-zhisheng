package com.shangbaishuyao.bean


/**
 * Desc: 根据布隆过滤器统计UV的输出的样例类 <br/>
 */
case class UVByBoomResultProcessOutPut (
                                         windowRange:String, //窗口范围
                                         count:Long    //用户统计的个数
                                       )