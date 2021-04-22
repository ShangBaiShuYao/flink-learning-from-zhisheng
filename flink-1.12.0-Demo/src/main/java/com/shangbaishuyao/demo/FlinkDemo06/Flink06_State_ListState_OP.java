package com.shangbaishuyao.demo.FlinkDemo06;

import com.shangbaishuyao.demo.Function.MyMapFunc2;
import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 *算子状态呢,他有三种基本数据的存储结构.这所说的是算子管理的这些状态有三个存储方式.
 * 第一种是列表状态(List state):就是说我们的算子状态中的数据是使用列表的方式.是存在一个列表里面的.把整个算子的所有数据以一组列表的方式存储起来.
 * 第二种是联合列表状态(Union List State):他也是以列表来存放算子状态的所有的数据.他和前面列表状态的区别是: 在发生故障时,或者从保存点启动应用程序去恢复数据的时候,他的运行代码不同.
 * 第三种是广播状态(Broadcast state):广播状态的意思就是说.我现在有一个算子.这个算子里面呢,他有一些数据或者说有一些逻辑,这个逻辑呢,其他的算子也是会用得到这个逻辑的.或者说这个数据,
 * 其他的算子也会用得到.那怎么办呢? 我们前面讲过,算子和算子之间的任务是不能共享的.这个时候呢,我们可以把这个状态存为一种广播状态,存为广播状态的话,这种情况下,他会把状态数据往其他的子任务上去发.这样的话,其他任务上也会有这个所谓的状态数据了.
 *
 * Author: shangbaishuyao
 * Date: 0:21 2021/4/23
 * Desc:
 */
public class Flink06_State_ListState_OP {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });
        //3.统计元素的个数
        waterSensorDS.map(new MyMapFunc2()).print();
        //4.执行任务
        env.execute();
    }
}
