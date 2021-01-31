package com.shangbaishuyao.connectors.flume;

import com.shangbaishuyao.connectors.flume.utils.FlumeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * Desc:
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 13:29 2021/1/31
 */
@Slf4j
public class FlumeSink<IN> extends RichSinkFunction<IN> {

    private static final int DEFAULT_MAX_RETRY_ATTEMPTS = 3; //最大尝试次数
    private static final long DEFAULT_WAIT_TIMEMS = 1000L; //等待时间
    private String clientType; // 客户端类型
    private String hostname; //主机名
    private int port; //端口号
    private int batchSize; //批处理大小
    private int maxRetryAttempts; //最大尝试重复次数
    private long waitTimeMs; //等待时间ms
    private List<IN> incomingList; //进入列表
    private FlumeEventBuilder eventBuilder; //时间生产器
    private RpcClient client; //客户端

    //构造函数(初始化,必须先初始化)
    public FlumeSink(String clientType,
                     String hostname,
                     int port,
                     FlumeEventBuilder<IN> eventBuilder,
                     int batchSize,
                     int maxRetryAttempts,
                     long waitTimeMs) {
        this.clientType = clientType;
        this.hostname = hostname;
        this.port = port;
        this.eventBuilder = eventBuilder;
        this.batchSize = batchSize;
        this.maxRetryAttempts = maxRetryAttempts;
        this.waitTimeMs = waitTimeMs;
    }

    //构造函数(设置默认值)
    public FlumeSink(String clientType,
                     String hostname,
                     int port,
                     FlumeEventBuilder<IN> eventBuilder) {
        this(clientType,
                hostname,
                port,
                eventBuilder,
                RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE,
                DEFAULT_MAX_RETRY_ATTEMPTS,
                DEFAULT_WAIT_TIMEMS);
    }

    //构造方法(设置默认值)
    public FlumeSink(String clientType,
                     String hostname,
                     int port,
                     FlumeEventBuilder<IN> eventBuilder,
                     int batchSize) {
        this(clientType,
                hostname,
                port,
                eventBuilder,
                batchSize,
                DEFAULT_MAX_RETRY_ATTEMPTS,
                DEFAULT_WAIT_TIMEMS);
    }


    //下面是操作Flink的富函数 -----  RichSinkFunction<IN>
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        incomingList = new ArrayList();
        //公共客户端接口发送数据到Flume
        client = FlumeUtil.getRpcClient(clientType, hostname, port, batchSize);
    }

    @Override
    public void invoke(IN value) throws Exception {
        int number;
        //同步添加连入列表
        synchronized (this) {
            if (null != value) {
                incomingList.add(value);
            }
            number = incomingList.size();
        }

        //当同步列表数量等于批量大小时刷新一次
        if (number == batchSize) {
//            flush();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        FlumeUtil.destroy(client);
    }





    //刷新的方法
    private void flush()  throws IllegalAccessError{
        //进入列表转为事件列表
        List<Event> events = new ArrayList<>();
        List<IN> toFlushList;//进入清单


        synchronized (this){
            if (incomingList.isEmpty()){
                return;
            }
            toFlushList = incomingList;
            incomingList = new ArrayList<>();
        }

        //①进入列表 转为 事件列表
        for (IN value : toFlushList){
            Event flumeEvent = this.eventBuilder.createFlumeEvent(value, getRuntimeContext());
            events.add(flumeEvent);
        }

        int retries = 0;
        boolean flag = true;

        //②尝试连接
        while (flag){
            //如果客户端对象为空 或者 最大尝试重复次数小于0  那么就将flag改为false
            if (null != client || retries > maxRetryAttempts) {
                flag = false;
            }

            //如果 最大尝试重复次数 大于等于 0 并且 客户端对象等于空
            if (retries <= maxRetryAttempts && null == client) {
                log.info("Wait for {} ms before retry/在重试之前等待{}ms", waitTimeMs);
                try {
                    Thread.sleep(waitTimeMs);
                } catch (InterruptedException ignored) {
                    log.error("Interrupted while trying to connect {} on {}/试图在{}上连接{}时被中断", hostname, port);
                }
                //连接
                reconnect();
                log.info("Retry attempt number {}/重试尝试数{}", retries);
                retries++;
            }
        }

        //③连接成功后尝试发送事件
        try {
            //发送一个{@linkplain Event Event}的列表到关联的Flume源。
            client.appendBatch(events);
        } catch (EventDeliveryException e) {
            log.info("Encountered exception while sending data to flume : {}", e.getMessage(), e);
        }

    }


    //连接方法
    private void reconnect() {
        //先销毁,在连接
        FlumeUtil.destroy(client);
        client = null;
        client = FlumeUtil.getRpcClient(clientType, hostname, port, batchSize);
    }

}
