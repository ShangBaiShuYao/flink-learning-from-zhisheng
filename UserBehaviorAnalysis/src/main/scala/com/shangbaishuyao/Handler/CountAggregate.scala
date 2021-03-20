package com.shangbaishuyao.Handler

import com.shangbaishuyao.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * Desc:根据商品ID分组累加每个商品的访问次数 <br/>
 * create by shangbaishuyao on 2021/3/20
 *
 * 这里面的方法add等这些方法是在窗口还没关闭之前就已经开始运行了,因为来一条数据就加进去,来一条数据就加进去.
 *
 * @Author: 上白书妖
 * @Date: 18:21 2021/3/20
 *
 *        case class UserBehavior(
 *        userId:Long,itemId:Long,
 *        categoryId:Int,
 *        behavior:String,
 *        timestamp:Long)
 */                                           //输入类型 累加器的初始sum值 输出类型
class CountAggregate extends AggregateFunction[UserBehavior,Long,Long]{
  //创建一个累加器 初始化为0
  override def createAccumulator(): Long = {0}
  //有一个用户数据进来之后,我们累加器直接加1
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator+1
  //得到结果
  override def getResult(accumulator: Long): Long = accumulator
  //合并, 两个分区各自累加号了,然后两个分区间进行合并
  override def merge(a: Long, b: Long): Long = a+b
}