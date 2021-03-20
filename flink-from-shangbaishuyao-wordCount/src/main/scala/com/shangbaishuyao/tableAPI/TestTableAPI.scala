package com.shangbaishuyao.tableAPI

import com.shangbaishuyao.source.{MyCustomerSource, SensorReader}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
/**
 * Desc:
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 14:28 2021/3/20
 */
object TestTableAPI {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[SensorReader] = env.addSource(new MyCustomerSource)

    //需要把Stream变成Table,首先需要一个Table的TableEnvironment
    //初始化Table的上下文
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //导入table库中的隐式转换
    import org.apache.flink.table.api.scala._
    //把DataStream变成Table
    val sensorReaderTable: Table = tableEnv.fromDataStream(stream,'sid,'s_time,'s_temp)

    //默认把case class中的属性当成字段
    val table: Table = sensorReaderTable.select(" sid, s_time,s_temp").where(" s_temp>0  ")

    table.printSchema()
    //把Table变成DataStream
    table.toRetractStream[SensorReader].print()
//    table.toAppendStream[SensorReader].print()
    env.execute()
  }
}
