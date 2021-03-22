package com.shangbaishuyao.Handler

import java.util

import com.shangbaishuyao.bean.{loginLog, loginWarnting}
import org.apache.flink.cep.PatternSelectFunction

//这一步的本质上的意义就是将PatternStream转变成DataStream. 此处不用传参
class LoginMonitorFunction extends PatternSelectFunction[loginLog,loginWarnting]{
  //这个方法刚好返回loginWarnting, 那么就是匹配到一个事件之后执行一次
  //匹配一个事件的意思就是,根据你定义好的模式去匹配的.如果发现有一个用户在两秒中内确实登录了他才会调用这个select方法
  override def select(map: util.Map[String, util.List[loginLog]]): loginWarnting = {
    //util.Map[String, util.List[loginLog]] 这个String代表模式中节点的名字.此模式中我定义了两个节点.一个begin一个next
    //为什么是map集合呢? 因为他有两个节点. 每一个节点都有一个固定的名字.就以这个名字作为键.value即util.List[loginLog]是当前这个节点匹配到的输入对象

    //拿到第一次登陆失败的一条数据. 这个数据就是一条登陆日志数据. 就叫做loginLog
    val firstLongFaill: loginLog = map.get("begin").iterator().next()//拿到第一个节点所匹配到的数据. 因为是;list集合,所以用到迭代器
    //匹配模式定义的第二个节点的匹配成功的数据. 说白了在这里就是和第一次失败紧挨着的下一条登陆失败的数据
    val secondLongFaill: loginLog = map.get("next").iterator().next()//拿到第一个节点所匹配到的数据. 因为是;list集合,所以用到迭代器
    //输出             用户id                    第一次时间            紧挨着第二次时间
    loginWarnting(firstLongFaill.userId,firstLongFaill.loginTime,secondLongFaill.loginTime)
  }
}
