package com.shangbaishuyao.Handler

import java.util.UUID
import com.shangbaishuyao.app.appMarket
import org.apache.flink.streaming.api.functions.source.SourceFunction
import scala.util.Random

/**
 * Desc:
 * create by shangbaishuyao on 2021/3/22
 * @Author: 上白书妖
 * @Date: 5:32 2021/3/22
 * @param maxElement 最多生成的数据条数
 */
class myAppSource(maxElement: Long) extends SourceFunction[appMarket]{
  //定义停止的条件, 就是什么时候数据不要再生成了
  var running : Boolean =true
  //定义渠道的数组
  var channelList = List("AppStore","huaweiStore","头条","xiaomiStore")
  //定义用户行为数组
  var behaviorList = List("CLICK","SEARCH","DOWNLOAD","INSTALL","UNINSTALL")
  //定义随机对象
  var r = new Random()

  //TODO 生成数据
  override def run(ctx: SourceFunction.SourceContext[appMarket]): Unit = {
    //需要累加器记录数据生成的条数
    var  count:Long = 0
    while (running && count<= maxElement){
      var userId :String = UUID.randomUUID().toString
      val market = new appMarket(userId, behaviorList(r.nextInt(behaviorList.size)), channelList(r.nextInt(channelList.size)), System.currentTimeMillis())
      count += 1 //累加计数器
      ctx.collect(market)
      Thread.sleep(20) //每生成一条数据等待一下
    }
  }
  //TODO 设置停止
  override def cancel(): Unit = {

  }
}