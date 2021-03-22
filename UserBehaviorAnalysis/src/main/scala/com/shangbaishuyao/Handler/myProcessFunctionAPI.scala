package com.shangbaishuyao.Handler

import com.shangbaishuyao.bean.{OrderEvent, OrderMsg}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

/**
 * 这种代码使用socket一个个测试是没有问题的. 但是会读取文件里面的日志,数据一起跑过来会出现. 我的onTimer里面的触发器还没来得及执行.
 * onTimer的执行时操作水位线的情况下执行的.是79行数据来了之后才会超过水位线. 你79行的数据没有来,你的水位线只停留在78行数据.我的
 * 创建时间是0949加上900秒之后小于我78行的1951. 小于1951的按道理是超时的.这时候他确实会执行.但是onTimer的执行,是还没来得及执行的
 * 时候.你79行数据就已经来了. 一进来,一定会进入到if(value.state.equals("pay") && createOrder != null){}这里面.进入到这里面的话
 * 就会把你刚刚的触发器,就是还没来的即onTimer的触发器给删了.所以就会出现这种全是订单正常的情况. 但是我如果数据是正常运行一条条来是不会
 * 出现这种情况的.
 * @param tag
 */
//当前的业务场景: 事件的时间是升序的,而且一个订单不会创建两次. 在大数据中为什么要做数据清洗呢? 就是把不正常的数据给过滤掉.所以按道理说不应该会出现数据不对的情况
//即不会出现同一个订单出现两次, 也不会出现同一个订单支付两次.
//一开始订单创建的话,我不能百分百保证他是保存成功了.所以我这个状态保存订单创建的哪些数据.
class myProcessFunctionAPI(tag: OutputTag[OrderMsg] ) extends KeyedProcessFunction[Long,OrderEvent,OrderMsg]{
  //这时候就需要想清楚,到底用状态保存什么东西:  既然你是属于正常支付或者超市未支付我是不知道的,所以我只能保存起来
  //所以我的状态是直接保存订单数据 ,即状态直接保存订单的创建数据
  //初始化状态
  lazy val createOrderStage: ValueState[OrderEvent] = getRuntimeContext.getState[OrderEvent](new ValueStateDescriptor[OrderEvent]("create-order", classOf[OrderEvent]))
  //保存某个订单执行触发器的时间
  //触发超时的时间执行的时间
  //初始化状态
  lazy val timeOutState: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("time-out", classOf[Long]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderMsg]#Context, out: Collector[OrderMsg]): Unit = {
    //从状态中取得create的订单
    val createOrder: OrderEvent = createOrderStage.value()
    //首先判断进入这里面的数据是create订单的,还是pay订单的
    //如果进来的是create的订单
    if(value.state.equals("create")&&createOrder == null){//订单首先创建,保存创建的订单,同时注册一个触发器,触发器的执行时间是从创建的时间开始15分钟之后
      //value就是当前创建的订单. 这就是保存订单状态
      createOrderStage.update(value)
      //这是订单创建的时间
      val overTime: Long = value.orderTime * 1000 + (15 * 60 * 100)
      //考虑一个,未来到15分钟内订单已支付的情况,如果已支付需要删除触发器,这就需要把触发器的时间也保存到状态中.如果不保存都不知道删除哪一个触发器
      //注册一个触发器
      ctx.timerService().registerEventTimeTimer(overTime)
      timeOutState.update(overTime)
    }
    //如果进来的是pay的订单
    if(value.state.equals("pay") && createOrder != null){//如果进来一个订单支付的事件,需要在主流中输出orderMsg,并且删除触发器,更新状态
      //TODO 注意: 如果source直接读取文件,当前触发器还没来得及执行的时候,34767订单的pay事件已经进入当前代码了,就会删除触发器.
      //TODO 解决办法就是再加一个判断.就是事件是否超时的判断
      if (timeOutState.value() > value.orderTime * 1000){//大于就表示订单还没有超时. 触发器执行时间大于订单支付时间表示没有超时.执行时就是最后一个超时时间
        ctx.timerService().deleteEventTimeTimer(timeOutState.value()) //删除一个触发器
        out.collect(OrderMsg(value.orderId,"订单支付正常",createOrder.orderTime,value.orderTime))//主流中输出
        //清空状态
        timeOutState.clear()
        createOrderStage.clear()
      }
    }
  }
  //触发器执行, 一定支付超时,输出到测流
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderMsg]#OnTimerContext, out: Collector[OrderMsg]): Unit = {
    //那之前保存的创建订单
    var order =  createOrderStage.value()
    if(order!=null){
      //如果order是不空的,而且触发器已经执行onTimer了.就以为这你肯定是超时的
      ctx.output(tag, OrderMsg(order.orderId,"该订单在15分钟内没有支付",order.orderTime,0))
      //清空之前的状态
      createOrderStage.clear()
      timeOutState.clear()
    }
  }
}
