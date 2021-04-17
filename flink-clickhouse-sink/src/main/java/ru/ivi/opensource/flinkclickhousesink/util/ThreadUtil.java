package ru.ivi.opensource.flinkclickhousesink.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public final class ThreadUtil {
    private static ThreadFactory pool;
    private ThreadUtil() {}
/*    public static ThreadFactory threadFactory(String threadName, boolean isDaemon) {
        //双重校验机制创建对象. 双重校验是解决我们的线程安全问题的方式.
        if(pool == null){
            synchronized (ThreadUtil.class){
                if (pool == null) {
                    pool = new ThreadFactoryBuilder()
                            .setNameFormat(threadName + "-%d")
                            .setDaemon(isDaemon)
                            .build();
                }
            }
        }
        return pool;
    }*/
    public static ThreadFactory threadFactory(String threadName, boolean isDaemon) {
        return new ThreadFactoryBuilder()
                .setNameFormat(threadName + "-%d")
                .setDaemon(isDaemon)
                .build(); //java的构造者模式（builder）
    }
    public static ThreadFactory threadFactory(String threadName) {
        return threadFactory(threadName, true);
    }

    public static void shutdownExecutorService(ExecutorService executorService) throws InterruptedException {
        shutdownExecutorService(executorService, 5);
    }
    public static void shutdownExecutorService(ExecutorService executorService, int timeoutS) throws InterruptedException {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            if (!executorService.awaitTermination(timeoutS, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                executorService.awaitTermination(timeoutS, TimeUnit.SECONDS);
            }
        }
    }
}
