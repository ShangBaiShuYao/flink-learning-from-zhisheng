package com.shangbaishuyao.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.shangbaishuyao.source.{MyCustomerSource, SensorReader}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/**
 * Desc: 数据写入msyql <br/>
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 13:14 2021/3/20
 */
object StreamToCustomerTest {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    //需求：把SensorReader 直接写入数据mysql
    val stream: DataStream[SensorReader] = streamEnv.addSource(new MyCustomerSource).map(s=>{
      var preid =s.id
      val id: String = preid.substring(preid.indexOf("_")+1)
      new SensorReader(id,s.timestamp,s.temperature)
    })

    //数据写入msyql
    stream.addSink(new MyCustomerSink)

    streamEnv.execute()
  }
  class MyCustomerSink extends RichSinkFunction[SensorReader]{
    var conn:Connection =_
    var pst :PreparedStatement =_
    override def open(parameters: Configuration): Unit = {
      conn=DriverManager.getConnection("jdbc:mysql://localhost/test","root","123123")
      pst=conn.prepareStatement("insert into t_sensor (id,sensor_time,termperature) values (?,?,?) on duplicate key update sensor_time=? , termperature=?")
    }

    //把SensorReader 写入到表t_sensor ,如果表中已经存在同样的id，修改数据，如果表不存在，则直接插入
    override def invoke(value: SensorReader, context: SinkFunction.Context[_]): Unit = {
      pst.setInt(1,value.id.toInt)
      pst.setLong(2,value.timestamp)
      pst.setDouble(3,value.temperature)
      pst.setLong(4,value.timestamp)
      pst.setDouble(5,value.temperature)
      pst.executeUpdate()
    }

    override def close(): Unit = {
      pst.close()
      conn.close()
    }
  }
}
