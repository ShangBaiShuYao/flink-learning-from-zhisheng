package com.shangbaishuyao.fransform

import java.sql.{Connection, DriverManager}

import com.shangbaishuyao.source.FromCustomerSource.MyCustomerSource
import com.shangbaishuyao.source.SensorReader
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 1、需求，给传入一写sensorReader对象（时间，温度） ==> SenserReader（包含ID）
 *  id从Mysql 中来。如果t_sensor表存在这一条记录，返回该条记录的id封装成一个新SensorReader。没有则插入返回自增的id
 */
object MyUDFTest {
  def getConnenction(): Connection = {
    DriverManager.getConnection("jdbc:mysql://localhost/test","root","123123")
  }

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //获取的数据库连接
    val conn: Connection = getConnenction()
    import org.apache.flink.streaming.api.scala._

    val stream: DataStream[SensorReader] = streamEnv.addSource(new MyCustomerSource) //从数据源读取的id是无效，必须从数据库中获取

    stream.map(new CustomerMapFunction(conn))

    streamEnv.execute()
  }

//  class CustomerMapFunction(conn:Connection) extends  MapFunction[SensorReader,SensorReader]{
//
//    override def map(value: SensorReader): SensorReader = {
//      //jdbc操作数据返回id
//      new SensorReader("",100,23.3)
//    }
//  }

  class CustomerRichMapFunction extends RichMapFunction[SensorReader,SensorReader]{
    /**
     * 转换数据
     * @param value
     * @return
     */
    override def map(value: SensorReader): SensorReader = {
      new SensorReader("",100,23.3)
    }

    /**
     * 生命周期初始化的回调方法
     * @param parameters
     */
    override def open(parameters: Configuration): Unit = {
      //初始化jdbcConnect，
    }

    /**
     * 关闭的回调方法
     */
    override def close(): Unit = {

    }
  }
}
