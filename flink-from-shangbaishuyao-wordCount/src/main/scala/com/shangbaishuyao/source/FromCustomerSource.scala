package com.shangbaishuyao.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import scala.util.Random
/**
 * Desc:
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 1:15 2021/3/20
 */
object FromCustomerSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[SensorReader] = env.addSource(new MyCustomerSource)
    stream.print()
    env.execute()
  }
  /**
   * 自定义的一个Source
   */
  class MyCustomerSource extends SourceFunction[SensorReader]{
    //是否终止数据流的标记
    var flag =true;

    /**
     * 从自定义的source中读取数据的方法，该方法只调用一次
     * 需求：每次从传感器中获取10条数据，2秒取一次
     * @param ctx
     */
    override def run(ctx: SourceFunction.SourceContext[SensorReader]): Unit = {
      val random = new Random()
      while(flag){ //如果流没有终止，继续获取数据
        1.to(10).map(i=>{
          //生产数据
          new SensorReader("sensor_"+i,(System.currentTimeMillis()/1000) +random.nextInt(10),random.nextGaussian() *50  )
        }).foreach( ctx.collect(_)) //发送数据
        Thread.sleep(2000)
      }
    }

    /**
     * 未来在某种条件下可以取消流，终止流
     */
    override def cancel(): Unit = {
      flag=false
    }
  }
}


