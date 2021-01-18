package com.shangbaishuyao.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.net.URI;
import static com.shangbaishuyao.common.constant.PropertiesConstants.*;

/**
 * Desc: 自定义设置 CheckPoint配置 的代码配置工具类 <br/>
 *
 * 知识了解补充:                                                                 <br/>
 *       ①ParameterTool简介        https://www.jianshu.com/p/a71b0ed7ef15      <br/>
 *       ②rocketsdb简介            https://www.jianshu.com/p/3302be5542c7      <br/>
 *
 *
 *@Author: 上白书妖
 *@Date: 2020/11/18 9:33
 */
public class CheckPointUtil {
   public static StreamExecutionEnvironment setCheckPointConfig(StreamExecutionEnvironment streamExecutionEnvironment, ParameterTool parameterTool) throws Exception{

       /**
        * 使用parameterTool工具类
        *
        * 使用这个省略了判空操作
        * public boolean getBoolean(String key, boolean defaultValue) {
        *         this.addToDefaults(key, Boolean.toString(defaultValue));
        *         String value = this.get(key);
        *         return value == null ? defaultValue : Boolean.valueOf(value);
        *     }
        */
       // ①streamCheckPoint的类型是: 内存(Memory)
       if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE,false) && CHECKPOINT_MEMORY.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())){
           //state 存放在内存中,默认是5M
           MemoryStateBackend memoryStateBackend = new MemoryStateBackend(5*1024*1024*100);
           //设置 checkPoint 周期时间
           streamExecutionEnvironment.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL,60000));
           //将其设置到Flink的启动环境中
           streamExecutionEnvironment.setStateBackend(memoryStateBackend);
       }

       /**
        *
        * public FsStateBackend(URI checkpointDataUri, int fileStateSizeThreshold) {
        *         this(checkpointDataUri, (URI)null, fileStateSizeThreshold, -1, TernaryBoolean.UNDEFINED);
        *     }
        *
        */
       //②streamCheckPoint的类型是: 分布式文件存储系统(HDFS)
       if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE,false)&& CHECKPOINT_FS.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())){
           FsStateBackend fsStateBackend = new FsStateBackend(new URI(parameterTool.get(STREAM_CHECKPOINT_DIR)), 0);
           streamExecutionEnvironment.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL,60000));
           streamExecutionEnvironment.setStateBackend(fsStateBackend);
       }

       /**
        * Desc: rocketsdb简介
        *   存储和访问数百PB的数据是一个非常大的挑战，开源的RocksDB就是FaceBook开放的一种嵌入式、持久化存储、KV型且非常适用于fast storage的存储引擎。
        *   传统的数据访问都是RPC，但是这样的话访问速度会很慢，不适用于面向用户的实时访问的场景。随着fast storage的流行，越来越多的应用可以通过在flash中管理数据并快速直接的访问数据。
        *   这些应用就需要使用到一种嵌入式的database。
        *   使用嵌入式的database的原因有很多。当数据请求频繁访问内存或者fast storage时，网路延时会增加响应时间，比如：访问数据中心网络耗时可能就耗费50ms，跟访问数据的耗时一样多，甚至更多。
        *   这意味着，通过RPC访问数据有可能是本地直接访问耗时的两倍。另外，机器的core数越来越多，storage-IOPS的访问频率也达到了每秒百万次，传统数据库的锁竞争和context 切换会成为提高storage-IOPS的瓶颈。
        *   所以需要一种容易扩展和针对未来硬件趋势可以定制化的database，RocksDB就是一种选择。
        *   RocksDB是基于Google的开源key value存储库LevelDB
        *
        *
        * public RocksDBStateBackend(String checkpointDataUri) throws IOException {
        * 		this(new Path(checkpointDataUri).toUri());
        *        }
        */
       //③streamCheckPoint的类型是: RocksDB
       if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE,false)&&CHECKPOINT_ROCKETSDB.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())){
           RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(parameterTool.get(STREAM_CHECKPOINT_DIR));
           streamExecutionEnvironment.setStateBackend(rocksDBStateBackend);
       }


       //设置 checkPoint 周期时间
       streamExecutionEnvironment.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL,60000));


       //高级设置 (这些配置也建议写成配置文件去读取,优先环境变量)
       //①设置 exactly-once 模式
       streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
       //②设置 checkPoint 最小间隔 500 ms
       streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
       //③设置 checkPoint 必须要在一分钟内完成,否则会被丢弃
       streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(60000);
       //④设置 checkPoint 失败时,任务不会fail,改 checkPoint 会被丢弃
       streamExecutionEnvironment.getCheckpointConfig().setFailOnCheckpointingErrors(false);
       //⑤设置 checkPoint 的并发度为1    单词: Concurrent 并发
       streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
       return streamExecutionEnvironment;
   }
}
