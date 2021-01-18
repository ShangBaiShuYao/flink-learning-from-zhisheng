package com.shangbaishuyao.common.utils;

import com.shangbaishuyao.common.constant.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.io.IOException;

/**
 * Desc: <br/>
 * 几乎所有的Flink应用程序，包括批处理和流处理，都依赖于外部配置参数，这些参数被用来指定输入和输出源(如路径或者地址)，系统参数(并发数，运行时配置)和应用程序的可配参数(通常用在自定义函数中)。 <br/>
 * Flink提供了一个简单的叫做ParameterTool的utility，ParameterTool提供了一些基础的工具来解决这些问题，当然你也可以不用这里所有描述的ParameterTool，                             <br/>
 * 其他框架如:Commons CLI和argparse4j在Flink中也是支持的。                                                                                                         <br/>
 *
 *
 * 知识了解补充:                                                                                                                                                                                       <br/>
 *    Flink官方文档                                                                                                                                                                                   <br/>
 *       ①TimeCharacteristic简介: https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_time.html                                                                                      <br/>
 *       ②ParameterTool简介:      https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/best_practices.html#parsing-command-line-arguments-and-passing-them-around-in-your-flink-application <br/>
 *
 *    非Flink官方文档: <br/>
 *       ①TimeCharacteristic简介: https://www.jianshu.com/p/904cf7b5ac44   <br/>
 *       ②Flink的重启策略:         https://www.jianshu.com/p/4be0fa07f29d   <br/>
 *       ③ParameterTool简介:      https://www.jianshu.com/p/3dcbd1b241a1   <br/>
 *
 *
 *@Author: 上白书妖
 *@Date: 2020/11/18 16:17
 */
public class ExecutionEnvUtil {

    public static final ParameterTool PARAMETER_TOOL =  createParameterTool();

    /**
     * Desc: ①获取你的配置值并传入ParameterTool中 <br/>
     *
     * @param args
     * @return parameterTool
     * @throws Exception
     */
    public static ParameterTool createParameterTool(final String[] args)throws Exception{

        ParameterTool parameterTool = ParameterTool
                //fromPropertiesFile: 去读取一个Properties文件，并返回若干key/value对
                //getResourceAsStream: 通过getResourceAsStream方法获取项目下的指定资源  https://www.jianshu.com/p/be1de0bedddc  https://www.jianshu.com/p/d293ab248052
                .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                //使用ParameterTool.fromArgs从命令行创建ParameterTool(比如--input hdfs:///mydata --elements 42)
                .mergeWith(ParameterTool.fromArgs(args))
                //使用ParameterTool.fromSystemProperties从system properties创建ParameterTool(比如-Dinput=hdfs:///mydata
                .mergeWith(ParameterTool.fromSystemProperties());
        return parameterTool;
    }




    /**
     * Desc: ②获取你的../application.properties下的配置值并传入ParameterTool中 <br/>
     *
     * @return parameterTool
     */
    private static ParameterTool createParameterTool(){
        try {
            ParameterTool parameterTool = ParameterTool
                    //fromPropertiesFile: 去读取一个Properties文件，并返回若干key/value对
                    //getResourceAsStream: 通过getResourceAsStream方法获取项目下的指定资源  https://www.jianshu.com/p/be1de0bedddc  https://www.jianshu.com/p/d293ab248052
                    .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    //使用ParameterTool.fromSystemProperties从system properties创建ParameterTool(比如-Dinput=hdfs:///mydata
                    .mergeWith(ParameterTool.fromSystemProperties());
            return parameterTool;
        }catch (IOException e){
            e.printStackTrace();
        }
        //使用ParameterTool.fromSystemProperties从system properties创建ParameterTool(比如-Dinput=hdfs:///mydata
        ParameterTool parameterTool = ParameterTool.fromSystemProperties();
        return parameterTool;
    }



    /**
     * Desc: ③在流式环境中准备定义好的ParameterTool <br/>
     *
     * @param parameterTool
     * @return streamExecutionEnvironment
     * @throws Exception
     */
    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception{
        //初始化流式环境变量
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM,5)); //设置默认的并行度
        streamExecutionEnvironment.getConfig().disableSysoutLogging();
        //Flink的重启策略  https://www.jianshu.com/p/4be0fa07f29d
        streamExecutionEnvironment.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000));//fixedDelayRestart设置重启策略(重启次数,延迟时间间隔)
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE,true)){
            streamExecutionEnvironment.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL,10000));
        }
        //使用env.getConfig().setGlobalJobParameters将ParameterTool的访问范围设置为global
        streamExecutionEnvironment.getConfig().setGlobalJobParameters(parameterTool);
        //TimeCharacteristic简介 https://www.jianshu.com/p/904cf7b5ac44  单词: characteristic 特征,特有
        //EventTime是以数据自带的时间戳字段为准，应用程序需要指定如何从record中抽取时间戳字段
        //指定为EventTime的source需要自己定义event time以及emit watermark，或者在source之外通过assignTimestampsAndWatermarks在程序手工指定
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return streamExecutionEnvironment;
    }
}
