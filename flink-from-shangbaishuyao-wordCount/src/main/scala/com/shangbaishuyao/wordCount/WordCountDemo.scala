package com.shangbaishuyao.wordCount

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 * Desc: 流处理案例 <br/>
 * create by shangbaishuyao on 2021/3/19
 * @Author: 上白书妖
 * @Date: 17:50 2021/3/19
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    //流式处理的上下文
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //批处理的上下文
//    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //读取数据得到DataStream
    val stream1: DataStream[String] = streamEnv.readTextFile("D:\\IdeaProjects\\0615Flink\\datas\\wc.txt")
    //调用DataStream的转换算子
    val stream2: KeyedStream[(String, Int), Tuple] = stream1.flatMap(
      line => {
        line.split(" ")
      }
    ).map((_, 1))
      .keyBy(0)
      //keyBy算子只做分组 ,参数可以传入整数（0,1,2,3,4,5,6，。。。） 代表字段的下标  ==keyBy(_.1)
    val stream3: DataStream[(String, Int)] = stream2.sum(1)
    //打印结果
    stream3.print(" WordCount的统计结果 ")
//    stream3.addSink()
    streamEnv.execute("wc") //启动流计算
  }
}
