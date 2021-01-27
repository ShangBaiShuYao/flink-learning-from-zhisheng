package com.shangbaishuyao.connectors.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import com.shangbaishuyao.connectors.akka.utils.ReceiverActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.Collections;

/**
 * Desc: flink akka <br/>
 *
 * AkkaSource源码: https://github.com/apache/bahir-flink/blob/master/flink-connector-akka/src/main/java/org/apache/flink/streaming/connectors/akka/AkkaSource.java#L81
 *
 * Akka简介:
 * https://www.jianshu.com/p/7d941a3b0ccb
 * https://blog.csdn.net/pml18710973036/article/details/87807387
 *
 * Akka是JAVA虚拟机平台上构建高并发、分布式和容错应用的工具包和运行时。
 * Akka用Scala语言编写，同时提供了Scala和Java的开发接口。
 * Akka处理并发的方法基于Actor模型，Actor之间通信的唯一机制就是消息传递。
 *
 * create by shangbaishuyao by on 2021-01-19
 * @Author: 上白书妖
 * @Date: 12:21 2021/1/19
 */
public class AkkaSource extends RichSourceFunction<Object> {
    private static  final  Logger LOG = LoggerFactory.getLogger(AkkaSource.class);

                              //单词: serialversionUID 变量名
    private static final long serialVersionUID = 1L;

    //--- Fields set by the constructor(由构造函数设置的字段)
    private final Class<?> classForActor;
    private final String actorName;
    private final String urlOfPublisher;
    private final Config configuration;

    //--- Runtime fields(运行时字段)
    private transient ActorSystem receiverActorSystem;
    private transient ActorRef receiverActor;
    protected transient boolean autoAck;

    /**
     * Desc: Creates {@link AkkaSource} for Streaming
     *
     * @param actorName    Receiver Actor name
     * @param urlOfPublisher    tcp url of the publisher or feeder actor
     * @param configuration
     */
    public AkkaSource(String actorName, String urlOfPublisher, Config configuration) {
        this.classForActor = ReceiverActor.class;
        this.actorName = actorName;
        this.urlOfPublisher = urlOfPublisher;
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        receiverActorSystem = createDefaultActorSystem();

        if (configuration.hasPath("akka.remote.auto-ack") && configuration.getString("akka.remote.auto-ack").equals("on")) {
            autoAck = true;
        } else {
            autoAck = false;
        }
    }

    //源码
    @Override
    public void run(SourceContext<Object> sourceContext) throws Exception {
        LOG.info("Starting the Receiver actor {}", actorName);
        receiverActor = receiverActorSystem.actorOf(
                Props.create(classForActor, sourceContext, urlOfPublisher, autoAck), actorName);

        LOG.info("Started the Receiver actor {} successfully", actorName);
        Await.result(receiverActorSystem.whenTerminated(), Duration.Inf());
    }

    //源码
    @Override
    public void close(){
        LOG.info("Closing source (关闭数据源)");
        if (receiverActorSystem != null) {
            receiverActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
            receiverActorSystem.terminate();
        }
    }

    @Override
    public void cancel() {
        LOG.info("cancelling akka source(注销 akka 源)");
        close();
    }

    /**
     * Creates an actor system with default configurations for Receiver actor.(使用接收方参与者的默认配置创建参与者系统)
     *
     * @return 4Actor System instance with default configurations(具有默认配置的参与者系统实例)
     */
    private ActorSystem createDefaultActorSystem(){
        String defaultActorSystem ="receiver-actor-system";

        Config finalConfig = getOrCreateMandatoryProperties(configuration);

        return ActorSystem.create(defaultActorSystem,finalConfig);
    }


    //接收或创建一个配置
    private Config getOrCreateMandatoryProperties(Config properties){
        if (!properties.hasPath("akka.actor.provider")){
             properties = properties.withValue("akka.actor.provider", ConfigValueFactory.fromAnyRef("akka.remote.RemoteActorRefProvider"));
        }

        if (!properties.hasPath("akka.remote.enabled-transports")){
             properties = properties.withValue("akka.remote.enabled-transports",ConfigValueFactory.fromAnyRef(Collections.singletonList("akka.remote.netty.tcp")));
        }

        return properties;
    }

}
