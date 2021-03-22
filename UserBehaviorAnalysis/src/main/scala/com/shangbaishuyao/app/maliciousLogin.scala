package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.LoginMonitorFunction
import com.shangbaishuyao.bean.{adClick, loginLog, loginWarnting}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Desc: 恶意登录监控 <br/>
 *
 * 需求: 在2秒内,如果有用户连续两次登录失败,则发出告警
 *
 * create by shangbaishuyao on 2021/3/22
 *
 * @Author: 上白书妖
 * @Date: 16:26 2021/3/22
 */
object maliciousLogin {
  def main(args: Array[String]): Unit = {
    //初始化环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //导入隐士转换
    import org.apache.flink.api.scala._

    //配置数据源
    val dataStream1: DataStream[loginLog] = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\LoginLog.csv")
      .map(line => {
        val arr: Array[String] = line.split(",")
        loginLog(arr(0).trim.toLong, arr(1).trim, arr(2).trim, arr(3).trim.toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[loginLog](Time.seconds(6)) { //延迟多少秒
      //指定具体的事件时间
      override def extractTimestamp(element: loginLog) = element.loginTime
    })
    //不要过滤. 要分组. 业务需求是某个用户连续两次登录失败.所以每个用户我都要去考虑.所以我要按照用户分组
      .keyBy(loginLog=>{
        (loginLog.userId)
      })
    //TODO CEP第一步: 定义模式pattern (这里需要导入flink CEP的库,就是依赖) 定义任何模式都要有一个begin
    //泛型表示对什么样的类型事件定义我们的模式 begin里面取名字
    val pattern: Pattern[loginLog, loginLog] = Pattern.begin[loginLog]("begin")
      //给他一个条件: 有一个人首先登陆失败.或者说有一条数据叫登陆失败的数据
      //第一次失败
      .where(loginLog => {
        loginLog.loginType.equals("fail")
      })
      //连续登陆失败
      //第二次失败
      .next("next").where(loginLog => { //将next换成followedby是中间可以穿插其他事件的
      loginLog.loginType.equals("fail")
      })
      //连续多少秒内登陆失败,所以要加时间
      .within(Time.seconds(2))

     //TODO ECP 第二步: 模式检测 即pattern检测
     //模式检测可以得到一个PatternStream的对象. 实际上就是通过模式检测将一个DataStream转化为PatternStream.最后再通过输出Alert再次还原成DataStream
     val patternStream1: PatternStream[loginLog] = CEP.pattern(dataStream1, pattern)

     //TODO 输出alert 这一步的本质上的意义就是将PatternStream转变成DataStream. 此处不用传参
     //尽量不直接使用匿名函数类因为可读新不高,直接新建一个函数继承他
     val resultDataStream: DataStream[loginWarnting] = patternStream1.select(new LoginMonitorFunction)

     //打印测试
     resultDataStream.print()

     //执行
     env.execute()
  }
}
