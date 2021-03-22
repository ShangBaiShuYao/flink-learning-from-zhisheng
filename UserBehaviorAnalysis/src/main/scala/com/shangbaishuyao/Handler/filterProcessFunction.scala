package com.shangbaishuyao.Handler

import com.shangbaishuyao.bean.adClick
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

/**
 * Desc: 使用Flink底层API,即ProcessFunction API<br/>
 * create by shangbaishuyao on 2021/3/22
 * TODO 过滤操作输入的是addClick,输出也是addClick.这样就保证我后面的开窗聚合就不用了改代码了
 * <p> 状态放在内存中 <p/>
 * @Author: 上白书妖
 * @Date: 5:18 2021/3/22
 */
//TODO 用于过滤用户的.当同一个用户点击同一个广告超过5次则过滤. 需要一个状态来保存改用户的点击次数,还需要用状态保存是否在黑名单中的信息
//TODO 考虑黑名单的有效时间,每天的00:00:00重置黑名单 思路:在每天的00:00:00的时候触发一个程序,把前一天的黑名单中的用户状态清除.注册一个事件.只要用户第一次进来我注册一个未来可能清空的黑名单
//当然我们也可以将用户的访问状态放到redis中
class filterProcessFunction (maxClick:Long,outPutTag: OutputTag[adClick]) extends KeyedProcessFunction[(Long,Long),adClick,adClick]{
  //需要一个状态来保存用户的点击次数. 因为是KeyedProcessFunction.也就是说分好组之后进入到这个聚合函数里面的
  //分好组之后进入的,事实证明进来的就是一组数据. 进来的是一组数据的话.那就是同一个用户id  值类型的
  //状态1 (用来保存用户的点击次数)
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("user_count", classOf[Long]))

  //状态2 (保存当前用户是否在测输出流中被输出过. 我们需要一个任务只能在侧输出流中输出一次) 默认值是false
  //默认侧输出流没有输出过
  lazy val isSendedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is_send", classOf[Boolean],false))

  //这个方法返回值是Unit,那么实际上我们就可以不返回. 这就是为什么我们使用它来做过滤的原因
  override def processElement(value: adClick, ctx: KeyedProcessFunction[(Long, Long), adClick, adClick]#Context, out: Collector[adClick]): Unit = {

    //考虑一: 你的用户是否已经过滤过了. 考虑二: 如果你的点击次数确实已经超过了,那我肯定要过滤.
     //从状态中将count次数拿出来
     var count: Long = countState.value()
     //来一条数据就点击了一次,所以要累加
     count+=1

     //黑名单清空:什么时候用户第一次进来呢? 一定是count=0的时候
     if(count==0){
       //第一次进来执行触发器.触发器什么时候触发呢?是第二天的00:00:00 ,这个是站在事件的角度,还是处理时间的角度?
       //处理时间角度,因为我没办法预估你事件时间到底是什么时候, 万一是一个月前的时间呢.或者前天的呢.所以一定是处理时间为准
       //处理时间为准,第二天00:00:00触发
       //计算处理时间的第二天的00:00:00的时间戳
       //ctx.getTimeService中拿到.TimerService(时间服务)提供了一些列的方法.来完成一些列的功能,哪些功能呢?比如:访问时间戳、watermark以及注册定时事件。还可以输出特定的一些事件.
       val currProcessingTime: Long = ctx.timerService().currentProcessingTime()//拿到当前处理时间的时间戳
       //时间戳除以一天的毫秒数转化为天. 因为要第二天的,所以我们加1,再次转为时间戳
       var ts = (currProcessingTime/(24*60*60*1000)+1) * (24*60*60*1000) //这种情况的触发器有个情况:就是第一天的24点前我狂刷单6次.24点一过我又能狂刷单6次.
       //如果我想要你第一次刷单6次后进入黑名单,我限制死了你只能24小时候才能再次刷单的话. 可以将上面时间的毫秒数也加上. 这个根据业务需求而定.
       //注册一个触发器. 注册了触发器,还得重写一个注册触发器的方法
       ctx.timerService().registerProcessingTimeTimer(ts) //用处理时间即registerProcessTimeTimer
     }


    //更新状态.
    countState.update(count)

     //判断是否超过阈值
     if (maxClick > count){ //表示点击次数还没有超过阈值
        //没有超过阈值,输出到主流中
        out.collect(value)
     }else{ //点击的次数达到阈值,输出到测流. 一个用户只要输出一次就可以了. 在测流中输出一次
        //主流是通过out.collect()输出. 测流是靠context上下文环境输出
        //查看今天是否已经被输出过了
        if(!isSendedState.value()){ //到达阈值,且从来没有在测流中输出
          ctx.output(outPutTag,value)
          isSendedState.update(true)//表示我已经发过了,下次就不用了发了
        }
     }
  }
   //执行触发器的方法,就是按照指定时间触发. 指定时间是:var ts = (currProcessingTime/(24*60*60*1000)+1) * (24*60*60*1000)
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), adClick, adClick]#OnTimerContext, out: Collector[adClick]): Unit = {
    //执行清空
    countState.clear()
    isSendedState.clear()
  }
}
