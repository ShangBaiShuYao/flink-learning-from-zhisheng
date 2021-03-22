package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.{myProcessFunctionAPI}
import com.shangbaishuyao.bean.{OrderEvent, OrderMsg}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}

/**
 * <p>这里面代码有bug没解决,使用orderMonitor吧<p/>
 *
 * Desc: 订单支付实时监控 , 使用Flink的状态编程,不使用Flink CEP,但是和CEP编程效果一样. 即正常支付的订单超过水位线可以直接输出Msg<br/>
 *       15分钟超时的订单需要在超时之后输出msg <br/>
 *
 * <p>需求: 监控用户的每一个订单是否在15分钟内支付,如果没有支付,通过侧输出流输出提示信息,如果已经支付,通过主流输出提示信息<p/>
 *
 * 订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。
 * 对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，
 * 网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。
 * 另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。
 *
 * create by shangbaishuyao on 2021/3/22
 * @Author: 上白书妖
 * @Date: 21:01 2021/3/22
 */
object orderMonitor2 {
  def main(args: Array[String]): Unit = {
    //初始化环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setParallelism(1)
    //导入隐士转换
    import org.apache.flink.api.scala._
    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //读取数据转化为样例类
    val KeyedStream: KeyedStream[OrderEvent, Long] = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\OrderLog.csv")
//    val KeyedStream: KeyedStream[OrderEvent, Long] = env.socketTextStream("hadoop102",7777)
      .map(line => {
        val arr: Array[String] = line.split(",")
        OrderEvent(arr(0).trim.toLong, arr(1).trim, arr(2).trim, arr(3).trim.toLong)
      })
      //设置时间语义和watermark水位线. 升序 设置订单的时间
      .assignAscendingTimestamps(OrderEvent => {
        OrderEvent.orderTime * 1000L //因为订单的时间是以秒为单位的,所以我们设置为毫秒为单位
      })
      //需要分组吗?我们监控每一个订单. 所以要分组. 不要过滤,因为所有的订单都要拿来用
      .keyBy(OrderEvent => {
        OrderEvent.orderId
      })
    //定义侧输出流的标签
    val tag: OutputTag[OrderMsg] = new OutputTag[OrderMsg]("pay-timeOut")

     //TODO 状态编程
     //因为常用到侧输出流,那么状态编程里面哪里常用到侧输出流呢? 使用触发器来输出侧输出流,只能是底层API. 即ProcessFunction API
     val resultDataStream: DataStream[OrderMsg] = KeyedStream.process(new myProcessFunctionAPI(tag))
     //主流输出
     resultDataStream.print("main")
     //测流输出
     resultDataStream.getSideOutput(tag).print("sideOutPut")
     //执行
     env.execute()
  }
}
