package com.shangbaishuyao.Handler

import com.shangbaishuyao.bean.adClick
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * Desc: 使用Flink底层API,即ProcessFunction API<br/>
 * create by shangbaishuyao on 2021/3/22
 * TODO 过滤操作输入的是addClick,输出也是addClick.这样就保证我后面的开窗聚合就不用了改代码了
 * <p> 状态放在redis中 </p>
 * @Author: 上白书妖
 * @Date: 5:18 2021/3/22
 */
//TODO 用于过滤用户的.当同一个用户点击同一个广告超过5次则过滤. 需要一个状态来保存改用户的点击次数,还需要用状态保存是否在黑名单中的信息
//TODO 考虑黑名单的有效时间,每天的00:00:00重置黑名单 思路:在每天的00:00:00的时候触发一个程序,把前一天的黑名单中的用户状态清除.注册一个事件.只要用户第一次进来我注册一个未来可能清空的黑名单
//当然我们也可以将用户的访问状态放到redis中
//TODO 基于redis的解决方案来处理黑名单的问题,黑名单有效时间严格按照24小时
//TODO 设计一个表保存: 该用户的点击次数. 开始时间(用户第一次点击开始) 表名: t_user_filter , key:userId,adId  ,  value:count,startTime
class filterProcessFunctionStatePutRedis(maxClick:Long, outPutTag: OutputTag[adClick]) extends KeyedProcessFunction[(Long,Long),adClick,adClick]{
  lazy val jedies:Jedis = new Jedis("127.0.0.1",6379)
  jedies.auth("123456")

  //一个用户的点击数据传进来了
  override def processElement(value: adClick, ctx: KeyedProcessFunction[(Long, Long), adClick, adClick]#Context, out: Collector[adClick]): Unit = {
    var key:String = value.userId+","+value.adId
    //首先应该从redis中把当前用户的数据查询出来
    val redisValue: String = jedies.hget("t_user_filter", key)

    //处理时间
    val currentProcessingTime: Long = ctx.timerService().currentProcessingTime()
    var count:Long =0
    var startTime:Long=0
    var timeOut:Boolean = false //表示当前的黑明单没有过期,还是在有效时间内

    //如果查询到数据
    if(redisValue!=null){
       count = redisValue.split(",")(0).toLong
       startTime = redisValue.split(",")(1).toLong //这个其实时间是个时间戳
       //判断startTime时间距离当前处理时间,超过24小时(之前的黑名单过期了),需要把次数和时间再次初始化.用现在的处理时间作为过期时间了.
       //86400000是一天的毫秒数.就是起始时间加上一天的毫秒数,说白了就是加上24小时
       if (startTime+86400000 <= currentProcessingTime){ //大于等于当前处理的时间,表示黑名单过期了
         count = 0
         startTime = currentProcessingTime  //重置
         //过期,我们将标志改为true
         timeOut = true
       }
    }else{ //没有插到数据就是当前用户第一次进来. 即当前用户第一次点击
        startTime = currentProcessingTime
    }
    count+=1 //知道进入这个方法,就说明有一条新的点击进来了.就在原来的点击基础上加1

    if(count < maxClick){ //点击次数小于阈值,输出到主流
      out.collect(value)
    }else if(count == maxClick){ //点击次数等于阈值. 当前用户第一次到达阈值,输出到测流,所以输出到测流也只有一次
      ctx.output(outPutTag,value)
    }
    //跟新redis中的数据
    jedies.hset("t_user_filter", key,count+","+startTime)
  }
}
