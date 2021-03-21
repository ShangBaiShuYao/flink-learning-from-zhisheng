package com.shangbaishuyao.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.shangbaishuyao.bean.UserBehavior
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * Desc: 网站独立访客数（UV）的统计 <br/>
 *
 * 统计UV就是统计一个小时内,整个网站的UV
 * 分析: 实际上只需要用到用户id就可以了.其他数据根本用不到,
 *
 *
 * 如果数据量小我们直接使用set去重就可以了,set他是先将数据读取到内存中做去重. 但是数据量大的话,就不能使用Set去重.
 * 如果redis集群多的话,我们可以使用redis做去重.
 * 这里使用的是布隆过滤器器进行去重. 但是布隆过滤器有个问题,就是他统计出来的UV的准确性并不是百分之百准确.大概是百分之九十九点九的准确.
 * 假设,我们有十亿五千四百六十五个用户, 但是经过布隆过滤器之后他统计出来的是十亿五千四百六十个用户.这就是很小的差别.所以他不是百分之百准确.
 * 如果你想要做到百分之百,你可以使用redis. redis绝对可以做到百分之百. 但是redis特别耗费资源. 如果你需求允许百分之九十九点九的准确. 你就可以使用布隆过滤器.
 * 一般情况下,是允许百分之九十九点九的准确的.
 *
 * UV:
 * 同一用户的浏览行为会被重复统计。而在实际应用中，我们往往还会关注，在一段时间内到底有多少不同的用户访问了网站。
 * 另外一个统计流量的重要指标是网站的独立访客数（Unique Visitor，UV）。
 * UV指的是一段时间（比如一小时）内访问网站的总人数，1天内同一访客的多次访问只记录为一个访客。
 * 通过IP和cookie一般是判断UV值的两种方式。当客户端第一次访问某个网站服务器的时候，
 * 网站服务器会给这个客户端的电脑发出一个Cookie，通常放在这个客户端电脑的C盘当中。
 * 在这个Cookie中会分配一个独一无二的编号，这其中会记录一些访问服务器的信息，
 * 如访问时间，访问了哪些页面等等。当你下次再访问这个服务器的时候，
 * 服务器就可以直接从你的电脑中找到上一次放进去的Cookie文件，
 * 并且对其进行一些更新，但那个独一无二的编号是不会变的。
 * 当然，对于UserBehavior数据源来说，我们直接可以根据userId来区分不同的用户。
 *
 * 将数据映射到布隆过滤器中之后,他就不用在继续存了.下一次还拿相同数据过来,映射到布隆过滤器中,那么他返回的是true. 因为它里面已经有1了,有1就是ture.有1是ture的话,
 * 那我就认为你这个数据已经有了.我就不用在继续累加了.因为我要保证唯一性. 如果他返回是0. 0就是false.返回零则认为这个数据没有,没有就累加一下.累加器加个1.
 * 但是布隆过滤器有一个缺点就是. 有可能某两条数据通过hash算法的值算出来是一样的.既然是一样的话.那这两个都返回的是1. 那就溜掉一个新的用户没有统计进去.所以统计出来的
 * 结果只会小于或者等于真实的结果.
 *
 * 具体实现过程:
 * 就是说,我们不需要保存用户的id,我们只需要将用户id通过hash在和一个布隆的向量进行位运算.
 * 然后我们就可以用它来判断是否出现重复的id就可以了.所以本质上来说.
 * 布隆过滤器就是一种数据结构.他是一个很长的二进制向量.
 * 这个长度可以由你自己来设定.这就是一个标准.这个向量我们还需要找个地方存起来.
 * 而redis里面就有默认存二进制向量的命令.Setbit.和Gitbit.
 * 所以我们只需要将我设定的布隆向量存到redis中.
 * 注意:redis里面不是存用户id,而是二进制向量.
 * 然后每次我得到一个用户id,然后再得到用户的hash值.
 * 而后在去redis中去和这个二进制向量进行位计算.来返回0或者1.
 * 而redis中的gitbit实际上就是来帮我们做位计算的一个方法.
 * 实际上位计算的方法自己就不用写了. 这就是redis的好处.
 *
 * create by shangbaishuyao on 2021/3/21
 *
 * @Author: 上白书妖
 * @Date: 14:10 2021/3/21
 */
object UV {
  def main(args: Array[String]): Unit = {
    //初始化环境变量
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //指定时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.streaming.api.scala._
    //读取数据 , 并且设置时间语义和waterMark(水位线)
    //    env.readTextFile(getClass.getResource("").getPath)
    //83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
    //83.149.9.216 - - 17/05/2015:10:05:43 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png
    val stream: DataStream[UserBehavior] = env.readTextFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\UserBehavior.csv")
      //处理进来的每一条数据,转化为样例类对象;转化完成之后设置时间语义和waterMark
      .map(line => {
        //按照空格切分
        val arr: Array[String] = line.split(",")
        //向对象中填充数据
        new UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000) //因为这个数据是升序的,我们不是用乱序的方式,直接使用assignAscendingTimestamps
    //统计UV
    //timewindow.他加了一个all是什么意思呢?是一个全量的开窗函数.全数据的开窗函数.什么叫全数据的开窗函数呢?就是如果你的数据不是键值对的就调用他.
    // 只要有一条数据来了,我们就认为有一个访问了,就是一个pv了.但是我们需要做过滤
    stream.filter(userBehavior => {
      userBehavior.behavior.equals("pv")
    }).map(userBehavior=>{
      ("uv",userBehavior.userId)
    }).keyBy((_,1)) //keyBy就是拿到当前一个小时内存的所有的用户id
    //开窗: 这里我只需要使用滚动窗口就可以了.窗口有个触发机制. 就是得到了水位线的时候触发.或者你的迟到的数据来了之后也会触发.
      //迟到了数据来了也会触发的,这个是一个默认的触发机制. 这个默认的窗口触发机制里面他会自动他窗口里面所需要用到的数据保存起来
      //并且不会清空. 这个窗口数据在什么时候清空呢? 就是,比如你设置了迟到数据,一旦迟到数据触发计算之后,等迟到时间一过.那么这个状态数据直接清空掉
      //因为Flink他不可能状态数据一直保存的,Flink的状态一直保存在哪里呢? 只有在内存中,也只有在内存中.除非你用到RocksDB(将所有状态序列化后，存入本地的RocksDB中存储。)
      //如果没有RocksDB,那么就是保存在内存中, 但是内存是有限的,不可能都已经不要了.这个窗口都已经结束了还一直保存着状态.这是不可能的.
      //加了延迟的话,第一次触发不会清空. 只有超过等待的时间才会清空状态stage.
      //为什么说状态是什么时候清空的呢? 因为我们使用的是布隆过滤器. 布隆过滤器就是解决海量用户不存储的问题.
      //来一条,用一次之后就不存了.但是默认情况下,窗口什么时候将状态清空的呢?是窗口最后触发的时候.
      //如果你设置了等待的话.他还要等延迟数据来了之后才触发.如果你没有设置延迟等待的话. 这个就在你窗口触发的时候就把状态清理掉.
      //在这个窗口没有触发的时候,有源源不断的数据过来. 这个源源不断的数据是需要保存在状态里面的.但是我们这个窗口有多长呢?
      //我们的窗口有一个小时的时长. 这一个小时这么长的一个窗口而且,这一个小时的用户数量有上亿的用户数量.所以只要这个窗口没有真正意义上的
      //结束,即没有真正触发. 那么事实上,来一条用户数据的id就默认保存在在状态(stage)中. 这种情况下和我是不是用布隆过滤器是没有关系的.
      //因为,我们既然用到了布隆过滤器了, 我们是不是不要把用户id保存在状态中呢?是的.
      //默认情况下,窗口的状态会保存用户id,但是当用户数据上亿级别的时候,则不能保存.有什么特殊的办法呢?
      //我们就需要自定窗口的触发机制.就是当用户量达到上亿几倍的话,我不保存状态到内存.就是直接不保存
      //默认每个窗口都有自己的触发机制.这个默认的触发机制是根据你的延迟时间和watermark来决定的.
      //但是默认的触发机制在当前的场景下,是不适合的,所以我们需要自定义窗口触发机制
        .timeWindow(Time.hours(1))
      //自定义新的窗口触发机制
        .trigger(new MyTrigger())
      //接下来应该触发我们的窗口了. 就是定义我们触发窗口的函数
      //我们做的业务稍微复杂一下.我可以在触发这个窗口的函数里面做我们的布隆过滤.所以做布隆过滤的时候,处理的代码比较复杂一点.
      //就直接放大招process
        .process(new UVByBoomResultProcess())
        .print("main")
    //执行
    env.execute()
  }
}

//继承序列化接口的目的是,为了将这个对象放到其他地方保存. 比如文件中.或者redis中保存等
//参数是为了传一个很长很长的二进制向量进来
class MyBloomFilter(size:Int) extends Serializable{
  //这里面最重要的就是定义一个hash函数. ctrl+o
  //这就其实就是将userId当成一个字符串类型传进来. 然后这样就可以了
  //但是我为了保证每一次确定这个hash值不会出现hash冲突问题还可以传一个种子.这个种子可以设置,也可以不设置.你为了提高正确率的话.我设置一个种子
  //因为我们计算机没有真正的随机数.都是伪随机数. 除非你每次随机都传一个当前系统时间的时间戳作为种子. 这个种子一般是Long类型的
  //当然不传种子也没有关系. 这个不是必须的
  //改方法主要完成两件事情:1.根据用户id得到一个hash值, 2.和二进制向量进行位运算
  def  userIDHash(userId:String,seed:Long): Long ={
    //根据用户id得到hash值,我们自己规定一个算法. 就是根据用户userId中每一个字符的ASCII码值(阿克斯码值)乘以一个种子后再累加.
    //累加的原因是: 我的用户id(userId)有多个字符.
    // 这是我随便设置的你也可以不用ASCII值,也可以用其他的值
    //我也可以拿每一个字符的hashCode乘以种子 .
//    var hash:Long =0
//    for (i<-0 to userId.length-1){//遍历当前我用户id中的每一个字符. to:表示包头不包尾,所以要减去1
//        hash +=seed*seed+userId.charAt(i)
//    }
    //for循环之后才有真正的hash值. 所以for循环完成的就是根据我的用户id得到一个hash值.当然这个hash算法是我随便设置的一种算法
    //和二进制向量进行位运算
    //假如我的size是20. 这个20是20个byte. 我这20个byte需要将他变成二进制.所以我们需要移位运算
    //把hash值和size进行移位运算
    //为什么这里要减1呢? 因为是往左移动. 这样一移位.左边变就是用0来填充. 得到10000000.....一堆的0
    //但是我直接那10000000...和hash进行位运算的话,就不适合, 因为里面都是0.
    //所以我要减去1 .就变成 011111111... 为什么要这么一堆1呢? 就是要进行与运算我才能进行判断. 1和1进行与运算才能得到1
    userId.hashCode & ((1<<size) -1) //调用java中自己本身的hashcode算法.hash值变大,做大散列原则
  }
}



//每一个窗口处理完了之后我是不是需要输出这个窗口用户数量有多少个.也是输出一个二元组,如果你觉得二元组太麻烦了就可以自定义一个对象.我们直接自定义一个样例类
//处理数据. 在窗口触发的时候执行函数
class UVByBoomResultProcess() extends ProcessWindowFunction[(String,Long),UVByBoomResultProcessOutPut,String,TimeWindow]{

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

//窗口的开始和结束时间是包头不包尾的. 所以maxTimestamp最大时间戳的意思就是窗口的结束时间减去1毫秒
//我这里进来的是什么类型?是二元组 ("uv",userBehavior.userId)
//窗口是TimeWindow
class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
  //当元素进来的时候我怎么做   , 他需要返回一个TriggerResult. 那么这个TriggerResult里面有哪些东西呢?
  /**
   *
   * All elements in the window are cleared and the window is discarded,
   * without evaluating the window function or emitting any elements.
   * 翻译:
   * 窗口中的所有元素将被清除，窗口将被丢弃，不计算窗口函数或发送任何元素。
   *
   * PURGE(false, true);
   *
   * FIRE_AND_PURGE(true, true),这个就是清空状态中的数据 ,而刚好,我们就是来一条数据就要执行并且清空. 执行还是执行,但是执行完成之后不要保存了. 因为默认是保存的
   * fire_and_purge(true, true) 执行并且清空
   */
    //当窗口中进来一条数据之后,直接处理,处理完之后马上删除状态. 这样就不会把用户id保存到窗口的状态中.说白了就是状态不保存用户id,只保存你这算子本身的状态数据
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE //执行并且清空
  //如果处理时间到达了,这个不用管.但是你要返回一个东西. 返回继续执行操作.
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  //这个的第四就是说,当我真正需要清空的时候,你还需要做什么事情呢? 这里我们什么事情都不做
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}


/**
 * Desc: 根据布隆过滤器统计UV的输出的样例类 <br/>
 */
case class UVByBoomResultProcessOutPut (
                                         windowRange:String, //窗口范围
                                         count:Long    //用户统计的个数
                                       )