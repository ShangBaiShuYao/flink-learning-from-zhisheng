package com.shangbaishuyao.processFunction

import com.atguigu.flink.source.SensorReader
import com.shangbaishuyao.source.SensorReader
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 监控每一个传感器，如果温度再1秒内连续上升输出报警信息
 * 详细业务：再1秒内，传感器记录的温度没有任何一条持平或者下降。则称为1秒内温度连续上升
 * 如果警告已经触发，下一次的警告触发器和当前已经触发的警告中温度无关
 */
object TestKeyedProcessFunction {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1) //默认情况下每个任务的并行度为1
    import org.apache.flink.streaming.api.scala._
    //读取netcat流中数据 （实时流） ,读取的数据都是SensorReader
    var stream1= streamEnv.socketTextStream("hadoop101",7777).map(line=>{
      val arr: Array[String] = line.split(",")
      new SensorReader(arr(0),arr(1).toLong,arr(2).toDouble)
    })
    stream1.print("input data")
    //首先分组，调用底层的processFunctionAPI处理
    stream1.keyBy(_.id).process(new MyCustomerKeyedProcessFunction()).print("main")
    streamEnv.execute()
  }

  /**
   * 需要使用Flink的状态机制，保存上一条温度值
   */
  class MyCustomerKeyedProcessFunction extends KeyedProcessFunction[String,SensorReader,String]{
    //再open方法中初始化 或者，定义成lazy
    lazy val preState :ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("pre_state",classOf[Double]))
    //把注册触发器的时间记录在状态中
    lazy val timeState :ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time_state",classOf[Long]))
    override def processElement(value: SensorReader, ctx: KeyedProcessFunction[String, SensorReader, String]#Context, out: Collector[String]): Unit = {
      //从状态中得到上一条温度值
      var preTemperature:Double =preState.value()
      //得到当前的温度值
      var nowTemperature :Double =value.temperature
      //从状态中得到注册的触发器时间
      var time :Long =timeState.value()
      //得到当前处理的时间
      var currentTime :Long =ctx.timerService().currentProcessingTime()
      if(preTemperature==0.0){ //第一条数据刚进来，把第一条温度保存到状态中
        preState.update(nowTemperature)
      }else if( preTemperature>=nowTemperature && time!=0 ){ //如果温度持平或者下降，需要删除前期的触发器
        //删除触发器
        ctx.timerService().deleteProcessingTimeTimer(time)
        timeState.clear() //既然触发器已经删除，则情况状态
        preState.update(nowTemperature)
      }else if(preTemperature< nowTemperature && time==0){ //如果温度升高，则注册一个触发器
        //注册一个触发器，触发器的执行要延迟一秒
        ctx.timerService().registerProcessingTimeTimer(currentTime+1000)
        timeState.update(currentTime+1000)
        preState.update(nowTemperature)
      }
    }

    /**
     * 当前在某一秒内，真存在温度连续上升，则执行触发器
     * @param timestamp
     * @param ctx
     * @param out
     */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReader, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(ctx.getCurrentKey+" :温度连续上升了，发出警告!")
      timeState.clear()
      preState.clear()
    }
  }

}
