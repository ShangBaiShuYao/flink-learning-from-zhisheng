package com.shangbaishuyao.bean
/**
 * 广告统计分析的结果样例类
 * @param windowRange  窗口时间范围
 * @param province 省份
 * @param count 广告点击总数
 */
case class adClickResult(
                        windowRange:String,
                        province:String,
                        count:Long
                        )
