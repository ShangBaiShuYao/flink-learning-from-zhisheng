package com.shangbaishuyao.app

import com.shangbaishuyao.Handler.{MyBloomFilter, MyTrigger, UVByBoomResultProcess}
import com.shangbaishuyao.bean.{UVByBoomResultProcessOutPut, UserBehavior}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

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
//        .process(new UVByBoomResultProcess())
//        .print("main")
    //执行
    env.execute()
  }
}

