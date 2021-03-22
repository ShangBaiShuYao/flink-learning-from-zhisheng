package com.shangbaishuyao.Handler


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

