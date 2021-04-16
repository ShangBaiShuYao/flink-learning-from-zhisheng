package com.shangbaishuyao.gmall.realtime.app.DWM;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Desc: 独立访客UV的计算(当日的访客数)<br/>
 *
 * 前期准备：
 *   -启动ZK、Kafka、Logger.sh、BaseLogApp、UniqueVisitApp
 * 执行流程:
 *   模式生成日志的jar->nginx->日志采集服务->kafka(ods)
 *   ->BaseLogApp(分流)->kafka(dwd) dwd_page_log
 *   ->UniqueVisitApp(独立访客)->kafka(dwm_unique_visit)
 *
 * DWM层-访客UV计算
 * 需求分析与思路:
 *  UV，全称是Unique Visitor，即独立访客，对于实时计算中，
 *  也可以称为DAU(Daily Active User)，即每日活跃用户，
 *  因为实时计算中的uv通常是指当日的访客数。
 *  那么如何从用户行为日志中识别出当日的访客，那么有两点：
 *  其一，是识别出该访客打开的第一个页面，表示这个访客开始进入我们的应用
 *  其二，由于访客可以在一天中多次进入应用，所以我们要在一天的范围内进行去重
 *
 * 步骤: 核心的过滤代码
 *      ①首先用keyby按照mid进行分组，每组表示当前设备的访问情况
 *      ②分组后使用keystate状态，记录用户进入时间，实现RichFilterFunction完成过滤
 *      ③重写open方法用来初始化状态
 *      ④重写filter方法进行过滤
 *         可以直接筛掉last_page_id不为空的字段，因为只要有上一页，说明这条不是这个用户进入的首个页面。
 *         状态用来记录用户的进入时间，只要这个lastVisitDate(上次访问时间)是今天，就说明用户今天已经访问过了所以筛除掉。如果为空或者不是今天，说明今天还没访问过，则保留。
 *         因为状态值主要用于筛选是否今天来过，所以这个记录过了今天基本上没有用了，这里enableTimeToLive 设定了1天的过期时间，避免状态过大。
 *      ⑤将过滤处理后的UV写入到Kafka的dwm_unique_visit
 *
 * @Author: 上白书妖
 * @Date: 15:50 2021/4/9
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1  准备本地测试流环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //设置CheckPoint检查点超时时间
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置状态后端类型,保存在HDFS上
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"));
        //设置重启策略为不重启
        //env.setRestartStrategy(RestartStrategies.noRestart());


        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据进行结构的换换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

        //TODO 4.按照设备id进行分组
        KeyedStream<JSONObject, String> keybyWithMidDS = jsonObjDS.keyBy(
            jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );
        //TODO 过滤得到UV
        SingleOutputStreamOperator<JSONObject> filteredDS = keybyWithMidDS.filter(
            new RichFilterFunction<JSONObject>() {
                //定义状态
                ValueState<String> lastVisitDateState = null;
                //定义日期工具类
                SimpleDateFormat sdf = null;

                //上亿用户,没个用户都存一个状态,是不是压力比较大呢? 内存能抗住,大概需要2.5G左右.
                //但是为了不浪费,我们设置状态的过期时间.因为需求是统计日活的.
                @Override
                public void open(Configuration parameters) throws Exception {
                    //初始化日期工具类
                    sdf = new SimpleDateFormat("yyyyMMdd");
                    //初始化状态
                    //初始化状态的描述器,指定状态名称,执行状态类型
                    ValueStateDescriptor<String> lastVisitDateState = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                    //TODO 5.设置状态的过期时间
                    //因为我们统计的是日活DAU，所以状态数据只在当天有效 ，过了一天就可以失效掉
                    lastVisitDateState.enableTimeToLive(new StateTtlConfig.Builder(Time.days(1)).build()); //.build是构造者设计模式
                    this.lastVisitDateState = getRuntimeContext().getState(lastVisitDateState);
                }

                @Override
                public boolean filter(JSONObject stream) throws Exception {
                    //首先判断当前页面是否从别的页面跳转过来的
                    //说明是从别的地方跳转过来的, 肯定不是第一次访问. 统计日活就不统计在内了.
                    String lastPageId = stream.getJSONObject("page").getString("last_page_id");
                    //TODO 6.过滤访问的页面不是首页的情况.如果不为空则说明这不是进入APP的首页,A->B->C ,可能访问的是B页面.只保留进入的A页面.
                    if (lastPageId != null && lastPageId.length() > 0) {
                        //本身是过滤操作,然后返回false.相当于将这条记录干掉了.不要这条记录了.
                        return false;
                    }

                    //获取当前访问时间
                    Long ts = stream.getLong("ts");
                    //将当前访问时间戳转换为日期字符串
                    String logDate = sdf.format(new Date(ts));
                    //获取状态日期,即上次访问的日期
                    String lastVisitDate = lastVisitDateState.value();

                    //判断状态里面是否有值,如果有值.且上次访问时间和首次进入的时间相同,则说明今天已经访问过了.
                    //第一次访问页面的时间
                    //用当前页面的访问时间和状态时间进行对比
                    //TODO 7.查看进入A页面(首页)的时间和状态记录的是否一致. 一致,说明已经访问过,这不是今天第一次使用软件访问了.不同,则没有访问过,状态中记录今天第一次登陆的时间.方便下次做比较.
                    if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                        System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                        return false;
                    } else {//不相同,则说明今天没有访问过.更新状态的日期值.
                        System.out.println("未访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                        //TODO 8.是今天第一次访问,更新状态中的时间记录,以便下次比较
                        lastVisitDateState.update(logDate);
                        //return true就意味着将这条记录保留
                        return true;
                    }
                }
            }
        );
        //TODO 9. 向kafka中写回，需要将json转换为String
        //6.1 json->string
        SingleOutputStreamOperator<String> kafkaDS = filteredDS.map(jsonObj -> jsonObj.toJSONString());

        //6.2 写回到kafka的dwm层
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
