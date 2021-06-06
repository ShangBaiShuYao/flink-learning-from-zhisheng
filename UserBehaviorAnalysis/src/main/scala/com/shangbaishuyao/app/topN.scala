package com.shangbaishuyao.app

import java.util.Properties

import com.shangbaishuyao.Handler.{CountAggregate, HotItemAllWindowFunction, HotItemWindowFunction}
import com.shangbaishuyao.bean.UserBehavior
import com.shangbaishuyao.utils.{MyKafkaUtil, MyKafkaUtil2}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * 这个topN里面有bug ,使用topNFindBug这个里面没有bug
 *
 * Desc: 实时热门商品统计<br/>
 *
 * 其实就是用来统计我们所谓的热门的TopN的商品,
 * 不过呢他有一个要求:统计最近一个小时内的TopN.
 * 他有一个间隔,比如他有一分钟一次或者5分钟一次这么一个时间间隔.
 * 累加商品的访问次数取TopN.
 *
 * create by shangbaishuyao on 2021/3/20
 *
 * @Author: 上白书妖
 * @Date: 17:39 2021/3/20
 */
object topN {
  def main(args: Array[String]): Unit = {
    //初始化环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //导入隐士转换SourceFunction
    import org.apache.flink.streaming.api.scala._
    //指定时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //读取kafka数据
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers",MyKafkaUtil.bootstraps)
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","consumer-group1")
    props.setProperty("auto.offset.reset","latest")
    //kafka配置
    val kafkaProps: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](
      MyKafkaUtil.userBehaviorTopic,
      new SimpleStringSchema(), props)
    //添加数据源  测试的时候需要加上setStartFromEarliest()
    val kafkaStreamSource: DataStream[String] = env.addSource(kafkaProps.setStartFromEarliest())
    //将数据流转化为实体类对象
    val kafkaTouserBehaviorDataStream: DataStream[UserBehavior] = kafkaStreamSource.map(line => {
        //按照空格切分
        val arr: Array[String] = line.split(" ")
        //向对象中填充数据
        new UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
    })
    //指定EventTime具体是什么
    val streamToUserBehaviorTimestamp: DataStream[UserBehavior] = kafkaTouserBehaviorDataStream.assignAscendingTimestamps(_.timestamp*1000)
    //过滤,按照itemId分组,窗口大小,滑动步长
    val windowStream: WindowedStream[UserBehavior, Long, TimeWindow] = streamToUserBehaviorTimestamp.filter(_.behavior.equals("pv")).keyBy(_.itemId).timeWindow(Time.hours(1), Time.minutes(5))
    //数据在哪保存的呢? 数据是在状态中保存的.说白了就是在内存里面
    //聚合操作 , 聚合函数中，需要两个参数，第一个参数是做累加 ,第二个参数
                                      //商品的id,和当前商品的访问次数
    val aggregationDataStream: DataStream[(Long, Long)] = windowStream.aggregate(new CountAggregate, new HotItemWindowFunction)
    //把上一个窗口的结果数据做全量的排序计算 . 这个需要和前面的时间一致
    aggregationDataStream.timeWindowAll(Time.minutes(5))
      //用process做聚合
      //只有processWindowFunction或者processAllWindowFunction这两个底层的API才是做全窗口的一个函数
      .process(new HotItemAllWindowFunction(3))
      .print()
    //执行executor
    env.execute()
  }
}
