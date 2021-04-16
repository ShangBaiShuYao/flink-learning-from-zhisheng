package com.shangbaishuyao.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 优化2：异步查询 <br/>
 * Author: 上白书妖
 * Date: 2021/2/19
 * Desc:  创建单例的线程池对象的工具类
 *
 * 补充: 线程安全的问题:
 * synchronized: 加锁,让所有线程都去排队.
 * volatile: 它修饰的变量能够保证每个线程能够获取改变量的最新值,从而避免出现数据的脏读的现象.
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor pool;

    /**
     *     corePoolSize:指定了线程池中的线程数量，它的数量决定了添加的任务是开辟新的线程去执行，还是放到workQueue任务队列中去；
     *     maximumPoolSize:指定了线程池中的最大线程数量，这个参数会根据你使用的workQueue任务队列的类型，决定线程池会开辟的最大线程数量；
     *     keepAliveTime:当线程池中空闲线程数量超过corePoolSize时，多余的线程会在多长时间内被销毁；
     *     unit:keepAliveTime的单位
     *     workQueue:任务队列，被添加到线程池中，但尚未被执行的任务
     *     LinkedBlockingDeque: 无界的,意味着你设置的最大线程数量(maximumPoolSize)是没有效果的.
     *     ArrayBlockingDeque:有界的
     *     <Runnable>:这里放的是可执行的任务,这里我加个泛型.
     *     懒汉式: 线程不安全的.
     * @return
     */
    public static ThreadPoolExecutor getInstance(){
        //TODO 使用双重校验机制来创建对象. 双重校验是解决我们线程安全的方式.
        if(pool == null){ //这种加了判断能否保证这个对象是单例的嘛? 加了判断不能保证线程是安全的,必须加锁.
            //synchronized:不建议加在方法上面, 加在方法上影响范围比较大. 所以加在这里.所以现在这个是线程安全的了.
            synchronized (ThreadPoolUtil.class){
                if(pool == null){
                    pool = new ThreadPoolExecutor(
                            4,
                            20,
                            300,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return pool;
    }
}
