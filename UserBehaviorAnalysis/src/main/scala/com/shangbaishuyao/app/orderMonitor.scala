package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.{payOutTimeHandler, payOverHandler}
import com.shangbaishuyao.bean.{OrderEvent, OrderMsg}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Desc: 订单支付实时监控 , 使用Flink CEP编程<br/>
 *
 * <p>需求: 监控用户的每一个订单是否在15分钟内支付,如果没有支付,通过侧输出流输出提示信息,如果已经支付,通过主流输出提示信息<p/>
 * 主流和测流是分开的, 测流不需要等待15分钟
 *
 * CEP 是只要你的数据超过了水位线,就会触发一次. 两条一摸一样的数据. 第一条是12:05. 第二条进去是12:06. 这时候没有超过水位线,不会触发
 * 到三条是12:20 则超过水位线. 则触发窗口.
 *
 * 订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。
 * 对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，
 * 网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。
 * 另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。
 *
 * create by shangbaishuyao on 2021/3/22
 *
 * @Author: 上白书妖
 * @Date: 21:01 2021/3/22
 */
object orderMonitor {
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
//    val KeyedStream: KeyedStream[OrderEvent, Long] = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\OrderLog.csv")
    val KeyedStream: KeyedStream[OrderEvent, Long] = env.socketTextStream("hadoop102",7777)
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

     //Flink CEP 四步骤:
     //一. TODO 定义模式,只要匹配订单在创建之后15分钟内有支付的事件
     //订单创建的事件开头   OrderEvent.state.equals("create")表示有一个订单,首先他的状态是创建状态,即订单创建了,我才去匹配
     val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(OrderEvent => {
       OrderEvent.state.equals("create")
     }) //这是一个单一模式
       //第二个单一模式(followBy比next合适. 因为在没有支付之前,订单的状态可能被修改了一下金额或者收货地址.只要你在15分钟之后,我是允许你修改订单的)
       .followedBy("follow").where(_.state.equals("pay"))
       //在多长时间. 这15分钟之内, 如果订单产生了
       .within(Time.minutes(15))
      //三. TODO 模式检测
      val patternStream: PatternStream[OrderEvent] = CEP.pattern(KeyedStream, pattern)
    //四: TODO 生产Alter. 找到所有的在15分钟内支付的订单事件, 并且还要找到没有支付的订单事件

    //需要一个侧输出流的标签 ,不管是侧输出流还是主流,我们都定义一个提示信息, 标签一般都需要给个名字
    val sideOutputTag: OutputTag[OrderMsg] = new OutputTag[OrderMsg]("pay-timeOut")

    //只有调用select来输出一条alter. 但是这个select里面需要考虑超时的问题.就是在这15分钟内有订单创建.但是没有支付.我们称之为匹配超时
    //15分钟内,匹配成功,即订单支付了. 15分钟后,没有支付,即订单超时. 这两种我们都需要找到
    //                     侧输出流标签    一个是超时处理            一个是匹配成功处理
    val dataStream: DataStream[OrderMsg] = patternStream.select(sideOutputTag, new payOutTimeHandler(), new payOverHandler())
    //这个dataStream流就是主流
    dataStream.print("main")
    //侧输出流
    dataStream.getSideOutput(sideOutputTag).print("sideOutPut")
    //执行
    env.execute()
  }
}
