package com.shangbaishuyao.state

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/**
 * Desc:
 * create by shangbaishuyao on 2021/3/20
 * @Author: 上白书妖
 * @Date: 13:30 2021/3/20
 */
object TestCheckPointOnFsBackend {

  def main(args: Array[String]): Unit = {
    //流式处理的上下文
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //批处理的上下文
//    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._
    //设置CheckPoint
    // start a checkpoint every 1000 ms 每隔5000毫秒CheckPoint一次
    env.enableCheckpointing(5000)

    // advanced options:

    // set mode to exactly-once (this is the default) 设置CheckPoint 一致性级别为：精确一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // make sure 500 ms of progress happen between checkpoints 设置两次CheckPoint的间隔时间必须超过500毫秒
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    // checkpoints have to complete within one minute, or are discarded 设置CheckPoint的超时时间
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    //当前CheckPoint出现错误的时候，是否停止任务
    // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
    env.getCheckpointConfig.setFailOnCheckpointingErrors(true)

    // allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //设置状态后端,为HDFS的远程文件系统
    env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9000/my_checkpoint/"))

    //当任务cancel之后是否保存CheckPoint的fsStateBackenc目录
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //读取数据得到DataStream
    val stream: DataStream[String] = env.socketTextStream("hadoop101",7777)

   stream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print()


    env.execute("wc") //启动流计算

    //如果想测试恢复状态的过程，必须在生产环境中
    //步骤: 1、打包并flink run 命令启动job， 2、使用flink cancel 命令停止job 。3、flink run 再次启动

  }
}
