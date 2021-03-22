package com.shangbaishuyao.Handler

import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import com.shangbaishuyao.bean.UVByBoomResultProcessOutPut
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
import org.apache.flink.streaming.api.scala.function._

//每一个窗口处理完了之后我是不是需要输出这个窗口用户数量有多少个.也是输出一个二元组,如果你觉得二元组太麻烦了就可以自定义一个对象.我们直接自定义一个样例类
//处理数据. 在窗口触发的时候执行函数
class UVByBoomResultProcess extends ProcessWindowFunction[(String,Long),UVByBoomResultProcessOutPut,String,TimeWindow]{


  //我们在这个方法里面需要做bloom过滤器.所以我们需要做这一下几件是
  //1. 需要一个很长的二进制向量. 向量其实就是数组,你只要把他当成数组看就可以了.在计算机里面向量就可当成数组.向量保存在redis中,每次从redis中返回1或者0
  //如果你有一个数据进过hash已经和二进制向量进行位计算之后.之前计算过一次了而且存进去了,实际上下次在计算之后redis返回给我们的就是1了
  //如果每次他会从redis中返回1或者0. 我需要根据1或者0,来进行判断
  //我们需要一个很长的二进制向量和每一个用户id的hash值进行位运算,所以我们需要初始化一下hash方法,初始化一下二进制向量.如果我们不自己写hash
  //直接使用hashCode的值也是可以的.但是有点风险. hashcode的值是java中自带的. 我们是可以用hashCode的值的.因为hash值就得到一个Long类型就可以了
  //hashcode也是一个Long类型的.
  //但是这里我们自己写hash. 因为我们需要自己定义一个二进制向量的长度的

  //2.初始化redis连接,我可以重写一个open方法.为什么可以重写open方法呢?(ctrl+o) 因为我们是processWindowFunction.
  //我们可以在open方法中做一些初始化的操作
  var jedis: Jedis = _
  var bloom : MyBloomFilter=_
  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("localhost", 6379)
    //选择数据库
    jedis.select(3)
    bloom = new MyBloomFilter(30) //这个参数怎么传?
    // 就看你数据量有多大.你可以写一个test,打印一下println(1<<20).
    // 假设我size设置为20.就是在代码里面将1左移20位.
    // 当然这个打印出来是十进制,不是二进制的.
    // 因为代码中默认自动将二进制转为十进制. 1048576这个打印出来的是十进制的. 这时候你可以根据计算机算一下到底是多少.
    //1048576这个二进制向量占用多少空间呢? 因为他是以字节为单位的,所以需要除以1024. 1048576/1024=1024的到k.就是1M空间.
    //你想要存的更加大写可以改的跟大数字就好了. 比如20. 30 ....
  }

  //这里面的方法是来一条数据就执行一次
  //窗口触发执行其实就是执行这里面的方法
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UVByBoomResultProcessOutPut]): Unit = {
    lazy val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    var windowEnd = context.window.getEnd //long类型的
    var windowStart = context.window.getStart
    val windowRange: String = format.format(new Date(windowStart)) + "--" + format.format(new Date(windowEnd))
    //从redis中将累加器的值拿到
    var count:Long = 0
    if (jedis.hget("t_count",windowEnd.toString)!=null){
      count=jedis.hget("t_count",windowEnd.toString).toLong
    }

    //由于他前面没有预处理. 所以他来一条数据就会执行一次. 来一条数据就会执行一次
    //根据山下文得到,这个elements只是一条数据. 因为我们上面没有做任何预处理.所以来一条数据就会执行一次
    var userId = elements.last._2.toString  //得到用户id
    //当有一个用户id进来,首先判断,用户id和二进制向量位运算是否等于1.等于1实际上就是true.即表示存在了.存在就不需要做累加了.否则需要累加1.累加的
    //是用户的数据. 既然累加用户的数据.那这个用户的数据是需要保存的.就是用户累加器的那个值是需要保存的
    //用户数量累加器保存在哪里呢? 保存在open方法里面可以吧?不行,如果你考虑分布式的情况就不行了. 因为一个open即一个窗口是1个小时. 我也没有设置延迟操作.
    //如果我这是了延迟操作的话就不能写在open里面了. 因为我怕这个窗口会被触发很多次.所以写在open方法里面就会有风险.
    //如果考虑分布式的情况的话.假设有一个slot上运行了ProcessWindowFunction.而有可能在其他另外一个slot上也会运行这一段代码.所以用户累加器是
    //不能放在窗口的open方法里面的. 那Jedis为什么可以呢?Jedis放在这里表示一个Slot或者或者说一台机器和一个Jedis的连接.这是可以接受的.
    //但是累加器是一个全局的.站在分布式来说也是一个全局的.所以,用户数量的累加器必须是一个全局的变量
    //全局变量要不就使用广播变量.要不就借助外面的东西来存放. 比如redis.放到redis中是怎么保存的呢? 是一个窗口对应一个累加器. 因为一个窗口就是一个小时
    //我是需要统计这一个小时的UV.下一个小时的UV和上一个累加器是没有关系的.
    //这个种子也是可以传当前时间戳,因为当前时间戳也是一直在变化. 但是一直在变化的话在当前我这个环境下是不行的.因为同样一个id你的种子变了hash值也变了.
    //hash值变了你在进行位运算的话.很可能返回false.所以这个种子应该是固定的,即每次都传一样的.既然如此我就传一个确定的质数
    var offset:Long = bloom.userIDHash(userId,71)
    //接下来就调用redis里面的API进行判断了.为什么用redis呢? 除了方便保存外,它还提供了getbit()
    //这里为什用windowEnd即窗口结束时间呢? 因为一个窗口应该一个bloom过滤器.因为这个窗口所有用户算完了,这个窗口差不多有一亿条数据.下一个窗口不可能还用之前那个二进制向量
    //我需要一个全新的二进制向量.哪只能使下一个窗口了. 窗口用什么来区分呢? 就是用窗口时间来去分了. 窗口时间要么就是windowStart要么是windowEnd.
    val flag: lang.Boolean = jedis.getbit(windowEnd.toString, offset) //flag表示用户是否存已经累加过的标记
    //如果flag为true表示已经存进去一个了.不用累加了.不用累加我们可以输出一下,看看到底总共累加多少
    if (flag){//flag等于true的话,表示用户id不需要累计,直接输出值就可以了
      out.collect(UVByBoomResultProcessOutPut(windowRange,count))
    }else{
      //这里需要做累加
      count += 1
      //累加之后放到redis中去
      jedis.hset("t_count",windowEnd.toString,count.toString)
      //然后再次输出一下
      out.collect(UVByBoomResultProcessOutPut(windowRange,count))
    }
  }
}

