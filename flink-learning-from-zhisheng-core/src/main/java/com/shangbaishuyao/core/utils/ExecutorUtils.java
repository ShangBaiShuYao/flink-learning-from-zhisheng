/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.shangbaishuyao.core.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

//import static org.apache.flink.util.ExecutorUtils.gracefulShutdown;

/**
 * <br>此工具类编写的源码是仿造: import static org.apache.flink.util.ExecutorUtils.gracefulShutdown; 里面的<br/>
 *
 * Desc:  Utilities for {@link java.util.concurrent.Executor Executors}. <br/>
 *
 * 知识补充: <br/>
 *      JAVA多线程之-CompletableFuture: https://www.jianshu.com/p/4ccf7d19f1f3 <br/>
 *
 *
 * ExecutorUtils类说明: <br/>
 *      ①使用异步编排技术调用②优雅的关闭方法 <br/>
 *      使用 runAsync() 运行异步计算 如果你想异步的运行一个后台任务并且不想改任务返回任务东西， <br/>
 *      这时候可以使用 CompletableFuture.runAsync()方法，它持有一个Runnable对象，并返回 CompletableFuture。 <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/20 16:52
 */
public class ExecutorUtils {
        /**
         * java开发过程中经常需要打印日志信息，往往会在每个类的第一行加上形如以下代码：
         * protected static final Logger logger = LoggerFactory.getLogger(XXX.class);
         * 目的：使用指定的类XXX初始化日志对象，方便在日志输出的时候，可以打印出日志信息所属的类。
         * 示例：protected static final Logger logger = LoggerFactory.getLogger(XXX.class);
         *           logger.debug("hello world");
         * 输出：XXX:hello world
         */
        private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.util.ExecutorUtils.class);


    /**
     * 源码注释:<br/>
     *
     * Gracefully shutdown the given {@link ExecutorService}. The call waits the given timeout that
     * all ExecutorServices terminate. If the ExecutorServices do not terminate in this time,
     * they will be shut down hard.
     *
     * @param timeout to wait for the termination of all ExecutorServices
     * @param unit of the timeout
     * @param executorServices to shut down
     */

    /**
     * 上白书妖补充: <br/>
     *
     * ②Desc: 优雅地关闭给定的{@link ExecutorService}。调用等待给定的超时所有executorservice终止。如果ExecutorServices在此期间没有终止，他们将被彻底关闭。
     *
     * 单词: graceful 优雅的 executor 执行者
     *
     * @param timeOut  超时，以等待所有ExecutorServices的终止
     * @param unit  超时单位
     * @param executorServices  要关闭的executorServices
     */
    public static void gracefulShutdown(long timeOut, TimeUnit unit, ExecutorService... executorServices){
            for (ExecutorService executorService : executorServices){
                executorService.shutdown();
            }

            //被中断,被打断
            boolean wasInterrupted = false;
            //结束时间
            final long endtime = unit.toMillis(timeOut) + System.currentTimeMillis();
            //剩余时间
            long timeLeft = unit.toMillis(timeOut);
            //剩余时间大于0
            boolean hasTimeLeft = timeLeft > 0L; //因为是long型的,所以加上L

            for (ExecutorService executorService:executorServices){
                //如果有被中断或者剩余时间不大于0了
                if (wasInterrupted || !hasTimeLeft ){
                    //执行服务关闭
                    executorService.shutdown();


                }else { //如果没有被中断或者剩余时间还是大于0的
                    try {
                        /**
                         * awaitTermination(timeOut,TimeUnit.MILLISECONDS): 请求关闭
                         * 阻塞，直到所有任务在关闭请求后完成执行，或发生超时，或当前线程被中断(以最先发生的为准)。
                         *
                         * timeOut: 超时——等待的最大时间单位——超时参数的时间单位
                         * TimeUnit.MILLISECONDS: 如果终止此执行程序，则为true;如果终止前超时已过，则为false
                         */
                        //如果执行服务没有请求关闭
                        if (!executorService.awaitTermination(timeOut,TimeUnit.MILLISECONDS)){
                            LOG.warn("ExecutorService没有及时终止。现在就把它关掉。"+" ------> 英文翻译 -----> "+"ExecutorService did not terminate in time. Shutting it down now.");
                            executorService.shutdown();
                        }
                    }catch (InterruptedException e){
                            LOG.warn("在关闭执行程序服务时中断。现在关闭所有剩余的ExecutorServices。"+" ------> 英文翻译 -----> "+"Interrupted while shutting down executor services. Shutting all remaining ExecutorServices down now.",e);
                            executorService.shutdown();
                            //被中断
                            wasInterrupted = true;

                            //当前线程被中断
                            Thread.currentThread().interrupt();
                    }
                    // 剩余时间 = 结束时间 - 系统的当前时间
                    timeLeft = endtime - System.currentTimeMillis();
                    // 剩余时间 大于 0 吗
                    hasTimeLeft = timeLeft > 0L;
                }
            }
        }
        /**
         * 源码注释:<br/>
         *
         * Shuts the given {@link ExecutorService} down in a non-blocking fashion. The shut down will
         * be executed by a thread from the common fork-join pool.
         *
         * <p>The executor services will be shut down gracefully for the given timeout period. Afterwards
         * {@link ExecutorService#shutdownNow()} will be called.
         *
         * @param timeout before {@link ExecutorService#shutdownNow()} is called
         * @param unit time unit of the timeout
         * @param executorServices to shut down
         * @return Future which is completed once the {@link ExecutorService} are shut down
         */

        /**
         * 上白书妖补充:  <br/>
         *  ①Desc:  使用异步编排技术
         *         以非阻塞方式关闭给定的{@link ExecutorService}。关闭将由公共fork-join池中的一个线程执行。
         *         executor服务将在给定的超时时间内优雅地关闭。以后 {@link ExecutorService # shutdownNow()} 将被调用。
         *
         * @param timeout 超时,在{@link ExecutorService#shutdownNow()}被调用之前
         * @param unit 超时的时间单位
         * @param executorServices 去关闭
         * @return completableFuture 异步编排,当{@link ExecutorService}被关闭时，它就完成了
         */
        public static CompletableFuture<Void>  nonBlackingShutDown(long timeout , TimeUnit unit , ExecutorService... executorServices){

            //使用异步编排技术
            //JAVA多线程之-CompletableFuture https://www.jianshu.com/p/4ccf7d19f1f3
            /**
             * 使用 runAsync() 运行异步计算 如果你想异步的运行一个后台任务并且不想改任务返回任务东西，
             * 这时候可以使用 CompletableFuture.runAsync()方法，它持有一个Runnable对象，并返回 CompletableFuture。
             */
            CompletableFuture<Void> completableFuture = CompletableFuture.supplyAsync(
                    () -> {
                        //调用上面优雅的关系方法
                        gracefulShutdown(timeout, unit, executorServices);
                        return null;
                    }
            );
            return completableFuture ;
        }
}
