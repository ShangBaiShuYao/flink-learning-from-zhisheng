package com.shangbaishuyao.Handler

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

//窗口的开始和结束时间是包头不包尾的. 所以maxTimestamp最大时间戳的意思就是窗口的结束时间减去1毫秒
//我这里进来的是什么类型?是二元组 ("uv",userBehavior.userId)
//窗口是TimeWindow
class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
  //当元素进来的时候我怎么做   , 他需要返回一个TriggerResult. 那么这个TriggerResult里面有哪些东西呢?
  /**
   *
   * All elements in the window are cleared and the window is discarded,
   * without evaluating the window function or emitting any elements.
   * 翻译:
   * 窗口中的所有元素将被清除，窗口将被丢弃，不计算窗口函数或发送任何元素。
   *
   * PURGE(false, true);
   *
   * FIRE_AND_PURGE(true, true),这个就是清空状态中的数据 ,而刚好,我们就是来一条数据就要执行并且清空. 执行还是执行,但是执行完成之后不要保存了. 因为默认是保存的
   * fire_and_purge(true, true) 执行并且清空
   */
  //当窗口中进来一条数据之后,直接处理,处理完之后马上删除状态. 这样就不会把用户id保存到窗口的状态中.说白了就是状态不保存用户id,只保存你这算子本身的状态数据
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE //执行并且清空
  //如果处理时间到达了,这个不用管.但是你要返回一个东西. 返回继续执行操作.
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  //这个的第四就是说,当我真正需要清空的时候,你还需要做什么事情呢? 这里我们什么事情都不做
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

