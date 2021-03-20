package com.shangbaishuyao.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 * Desc: 自定义的一个Source
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 13:02 2021/3/20
 */
class MyCustomerSource extends SourceFunction[SensorReader]{
  //是否终止数据流的标记
  var flag =true;
  /**
   * 从自定义的source中读取数据的方法，该方法只调用一次,那既然只调用一次,我们就需要在run方法里面写循环,这样流就会源源不断的来
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