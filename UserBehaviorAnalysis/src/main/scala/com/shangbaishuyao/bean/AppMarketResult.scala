package com.shangbaishuyao.bean


//APP市场推广按照渠道分组分析的结果
case class AppMarketResult(
                            WindowRange:String,
                            channel:String,
                            behavior:String,
                            userCount:Long
                          )
