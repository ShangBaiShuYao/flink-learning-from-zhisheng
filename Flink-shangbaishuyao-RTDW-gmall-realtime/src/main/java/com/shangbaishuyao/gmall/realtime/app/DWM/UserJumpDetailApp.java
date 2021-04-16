package com.shangbaishuyao.gmall.realtime.app.DWM;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * TODO Flink 的1.12版本 他的默认时间就是事件时间了。不需要设置了。
 *
 * Desc: 用户跳出行为过滤 <br/>
 *
 * 引流过来的访客是否能够很快被吸引. 比如学校招生网站外挂了许多年渠道.渠道如: B站,百度广告,知乎,头条等等.看看哪一个渠道引流过来的用户质量很好一点.
 * 比如:百度里引流的客户都只是浏览了首页,但是没有几个进入第二页或者第三页等等.
 *      而B站过来的用户几乎都浏览到了最后一页.
 *
 *
 * web页面默认的session会话超时时间是 30分钟.
 * A(首页) -----> B -----> C
 * 如果一个人进入首页,三十分钟没有再继续下一页,那么认为这人跳出去了. 下次进来重新计算.
 *
 * <p>需求分析与思路:</p>
 * 什么是跳出?
 * 跳出就是用户成功访问了网站的一个页面后就退出，不在继续访问网站的其它页面。而跳出率就是用跳出次数除以访问次数。
 * 关注跳出率，可以看出引流过来的访客是否能很快的被吸引，渠道引流过来的用户之间的质量对比，对于应用优化前后跳出率的对比也能看出优化改进的成果。
 *
 * <p>计算跳出行为的思路(跳出率是在DWS层做的)</p>
 * 首先要识别哪些是跳出行为，要把这些跳出的访客最后一个访问的页面识别出来。那么要抓住几个特征：
 * 该页面是用户近期访问的第一个页面
 *      这个可以通过该页面是否有上一个页面（last_page_id）来判断，如果这个表示为空，就说明这是这个访客这次访问的第一个页面。
 * 首次访问之后很长一段时间（自己设定），用户没继续再有其他页面的访问。
 *      这第一个特征的识别很简单，保留last_page_id为空的就可以了。
 *      但是第二个访问的判断，其实有点麻烦，首先这不是用一条数据就能得出结论的，需要组合判断，要用一条存在的数据和不存在的数据进行组合判断。
 *      而且要通过一个不存在的数据求得一条存在的数据。
 *      更麻烦的他并不是永远不存在，而是在一定时间范围内不存在。那么如何识别有一定失效的组合行为呢？
 *      最简单的办法就是Flink自带的CEP技术。这个CEP非常适合通过多条数据组合来识别某个事件。
 *
 * 用户跳出事件，本质上就是一个条件事件加一个超时事件的组合。
 *
 * <p>
 *     因为最终是要统计跳出率的,这个跳出率是在DWS层统计的.但是这跳出率是要按照维度来进行统计的. 比如:访客维度统计跳出率或者其他维度统计跳出率
 *     但是要计算跳出率,我们就得使用分子和分母相除. 分子是跳出次数. 分母是页面总访问次数.  总访问此处是在DWD层可以拿到.
 *     但是跳出行为,以及跳出的次数我们没有,不知道那些是跳出行为.所以我们需要从页面日志中拿到跳出行为.这就是这层DWM要做的事情.这里并不涉及计算,只是几率跳出行为.
 * </p>
 *
 *  跳出行为: ①第一次访问.②超过十秒没有访问别的页面. 将这个两个条件组合到一块. 就用到Flink CEP编程,即复杂事件处理. 定义一个模式.
 *
 * @Author: 上白书妖
 * @Date: 15:50 2021/4/9
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1  准备本地测试流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.2 设置并行度
        env.setParallelism(4);

        //1.3 设置Checkpoint
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //设置检查点的超时时间
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置检查的状态后端,保存的位置
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/uniquevisit"))
        //开启了检查点之后重启策略默认是自动重启的,这里我们关闭重启策略.
//        env.setRestartStrategy(RestartStrategies.noRestart());
        //TODO 注意: Flink1.12版本,默认的时间语义就是事件时间了.已经不需要这么设置了.
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        String sinkTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
//        DataStreamSource<String> dataStream = env.addSource(kafkaSource);
        //TODO 测试数据
        DataStream<String> dataStream = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":150000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":300000} "
            );
        //TODO 3.对读取到的数据进行结构的换换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStream.map(jsonStr -> JSON.parseObject(jsonStr));
        //jsonObjDS.print("json>>>>>");
        //注意：从Flink1.12开始，默认的时间语义就是事件时间，不需要额外指定；如果是之前的版本，需要通过如下语句指定事件时间语义
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //TODO 注意: Flink1.12版本,默认的时间语义就是事件时间了.已经不需要这么设置了.
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //TODO 4. 指定事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObjDS.assignTimestampsAndWatermarks(
            //forMonotonousTimestamps这个相当于是没有乱序的情况.
            //forBoundedOutOfOrderness(10)这个是乱序的,里面的10是允许迟到的时间.
                //withTimestampAssigner.这个是指定那个字段是时间戳
                //<JSONObject>这个泛型模板必须加上,不然不知道这个T是什么,报红线.
            WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                        return jsonObj.getLong("ts");
                    }
                }
            ));
        //TODO 5.按照mid进行分组
        KeyedStream<JSONObject, String> keyByMidDS = jsonObjWithTSDS.keyBy(
            jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );

        /*
            计算页面跳出明细，需要满足两个条件
                1.不是从其它页面跳转过来的页面，是一个首次访问页面
                        last_page_id == null
                2.距离首次访问结束后10秒内，没有对其它的页面再进行访问
        */
        //TODO 6.配置CEP表达式
        Pattern<JSONObject, JSONObject> flinkCEPPattern = Pattern.<JSONObject>begin("first")
                //通过where指定具体条件内容
                //TODO ①定义第一个模式
                .where(
                        // 模式1:不是从其它页面跳转过来的页面，是一个首次访问页面
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                //获取last_page_id
                                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                                //判断是否为null 将为空的保留，非空的过滤掉
                                if (lastPageId == null || lastPageId.length() == 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                )
                //TODO ②定义第二个模式
                .next("next")
                .where(
                        // 模式2. 判读是否对页面做了访问
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                //获取当前页面的id
                                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                //判断当前访问的页面id是否为null
                                if (pageId != null && pageId.length() > 0) {
                                    return true;
                                }
                                return false;
                            }
                        })
                //①和②只是定义了一个模式, 符合模式返回ture, 但是这个模式是由时间限制的.这里设置两个模式之间的时间限制.
                //TODO 设置两个模式间的时间限制
                .within(Time.milliseconds(10000));


        //TODO 7.根据Flink CEP模式来对流进行筛选
        PatternStream<JSONObject> patternStream = CEP.pattern(keyByMidDS, flinkCEPPattern);
        //TODO 8.从筛选的流中,提取数据   将超时数据  放到侧输出流中
        OutputTag<String> timeOutputTag = new OutputTag<String>("timeOut"){};
        //模式提取数据有两个方法,select和flatSelect. 两者区别就是: select有返回值,是通过返回值向下输出的.而flatSelect是通过集合的方式向下输出的
        //TODO 也就是说select只能返回一个值.而flatSelect可以返回多个值.
        /*patternStream.select(new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> pattern) throws Exception {

            }
        })*/
        SingleOutputStreamOperator<String> sideOutputStream = patternStream.flatSelect(
                timeOutputTag,
                //匹配上了模式, 但是数据超时的, 即处理超时数据的.
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        //TODO 这个pattern本身是一个map集合. 这个map集合里面的key(String)代表符合咱们第一个条件的,first所有的数据放到一个list集合中.
                        //TODO 就是符合我们第一个模式条件的,即①定义第一个模式,条件的所有数据都放到List集合中了.
                        //我们如何去除数据呢? 就是拿出所有符合first的数据.
                        List<JSONObject> jsonObjectList = pattern.get("first");
                        //这个jsonObjectList是符合firs模式的全部记录的.我需要一条条拿出来.
                        //TODO 注意: 在timeout方法中的数据都会被参数1中到的timeOutput标签标记一下.
                        for (JSONObject jsonLine : jsonObjectList) {
                            //这个jsonline就是符合的一条记录的. 这一条记录其实就是代表了一次跳出.
                            out.collect(jsonLine.toJSONString());
                        }
                    }
                },
                //匹配上了模式,处理没有超时的数据.即正常数据.
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                        //没有超时的数据不再我们统计范围之内.所以这里不需要写入代码.
                    }
                }
        );
        //TODO 9.从侧输出流中获取超时数据
        DataStream<String> jumpDataStream = sideOutputStream.getSideOutput(timeOutputTag);

        //打印测试
        jumpDataStream.print();

        //TODO 10.将跳出数据写回到kafka的DWM层
//        jumpDataStream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
