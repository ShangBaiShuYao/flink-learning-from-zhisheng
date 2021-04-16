package com.shangbaishuyao.gmall.realtime.app.DWD;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Desc: 准备用户行为日志的DWD层 <br/>
 * @Author: 上白书妖
 * @Date: 16:53 2021/4/9
 */
public class BaseLog_ODS_SideOutput_DWD {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BaseLog_ODS_SideOutput_DWD.class);
    //定义kafka主题
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";
    public static void main(String[] args) throws Exception{

        //TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度(并行度如何设置合适? kafka分区数,因为从kafka读数据.并行度小于kafka分区数,那kafka分区没意义. 多了,就浪费了)
        env.setParallelism(4);

        //TODO 这个检查点(checkPoint),没5秒开启一次,exactly-once,端到端的一致性.
        //将checkpoint存到远程的持久化文件系统（FileSystem）上。而对于本地状态，跟MemoryStateBackend一样，也会存在TaskManager的JVM堆上。
        //设置检查点CheckPoint, 这相当于一个快照. 端到端的一致.
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);//5秒开启一次ck, 默认是精确一致
        //设置检查点的超时时间. 可设置可不设. 超过一分钟废弃.
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //保存检查点. 即设置状态后端 , 8020:这个是服务端地址. 不是web端地址.
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/baselogApp"));
        //设置操作hdfs的访问权限
        System.setProperty("HADOOP_USER_NAME","shangbaishuyao");

        //TODO 2.从kafka读取数据
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据格式进行转换         String->json  map做结构转换
        SingleOutputStreamOperator<JSONObject> kafkaDataStreamMapJSONObject = kafkaDataStream.map(
                //这里使用的匿名内部类; 还有更细粒度的控制,为函数类的方式; 还可以使用富函数进行周期性的控制;
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        return jsonObject;
                    }
                }
        );
//        kafkaDataStreamMapJSONObject.print();
        /**
         * kafka数据---->flink应用程序----->kafka: 这个过程的精准一次性如何理解?
         * Flink的 exactly-once
         * 最早的精确一致是在kafka里面有,kafka本身就有.
         * 有个幂等操作 将kafka的幂等操作和ack响应级别结合在一起. 来保证kafka的精准一次性. (这是生产者)
         * 消费者消费kafka数据如何保证精准一次性呢? exactly-once
         * 一个是事务. 一个是offset偏移量. 这两个做一个整体: 业务数据处理成功,你的偏移量也得改为成功.业务数据失败了,我要回滚,偏移量就不能改为成功.
         * 但是这有要求.不是所有数据库都支持事务的.  可能我业务数据到mysql里面,但是offset保存在里面不合适. 如果数据写到不支持事务的数据库呢?这就行不通了.
         * 这种就不让kafka自动提交这个偏移量offset了. 而是我们手动提交偏移量offset.  自动提交时5提交一次.
         * 手动提交什么时候提交呢?
         * 如果先做处理,在做提交. 不行.
         * 如果先改变偏移量,在做处理. 也不行.
         * <p/> 选着Flink的exactly-once至少一次. 这样只能保证数据不丢失.但是不能保证数据不重复. 那我们可以自己实现一个幂等操作.这是kafka本身来做的<p/>
         *
         * 如果我们不用kafka, 我现在Flink来做精准一次性. Flink做精准一次性就是指的整个体系. 从kafka(数据源)-->flink---->kafka.
         * Flink精准一次性要求, 我们的数据源有重置的机制. 一旦处理失败,可以重新发送.
         * Flink内部精准一次性保障. CheckPoint. 当我从kafka的不同分区中读取数据.把当前读取数据的偏移量(offset)保存到检查点里面去.
         * 如果某天消费失败. 可以从检查点获取数据. 出现故障,可以从检查点恢复.
         * 859 983 886
         */
        //TODO 4.识别新老访客  状态修复
        // 前端也会对新老状态进行记录，有可能会不准，咱们这里是再次做一个确认
        // 保存mid某天方法情况（将首次访问日期作为状态保存起来），等后面该设备在有日志过来的时候，从状态中获取日期
        // 和日志产生日志进行对比。如果状态不为空，并且状态日期和当前日期不相等，说明是老访客，如果is_new标记是1，那么对其状态进行修复

        //根据mid分区是否合理呢?  电商网站有上亿用户. 每一个用户时访问用不同的设备,这就可怕了,因为是根据设备id分组的.
        //跟可怕的是我在这个组里面维护了一个状态. 这个状态维护在内存中了. 那这能抗住吗? 一亿用户,每一个都在内存中维护一个状态.
        //这能抗住吗?内存能受得了吗?
        //一亿用户存设备id的话 约等于 2.4G的内存. 还可以. 没有什么太大的问题.放心用.

        // 4.1 根据mid来对日志进行分组
        //如果使用lambda表达式的话,就会有个讨厌的问题,就是泛型擦除.如果返回是泛型,就会出现.
        KeyedStream<JSONObject, String> midKeyedStream  = kafkaDataStreamMapJSONObject.keyBy(
                data -> data.getJSONObject("common").getString("mid")
        );
        //4.2 监控状态,根据我们的key来记录状态
        //算子状态和键控状态, 哪一个合适呢? 记录某一个设备的访问情况. 使用监控状态. 他有很多,有valueState[T],ListState[T],mapState[T]...
        //我们状态是保存那天访问的就可以了, 所以定义一个值状态就可以了.
        SingleOutputStreamOperator<JSONObject> jsonDataStreamWithFlag = midKeyedStream.map(
                //富函数有生命周期,可以用作做初始化
                new RichMapFunction<JSONObject, JSONObject>() {
                    //使用键控状态, 定义一个状态.
                    //定义mid的访问状态
                    //记录当前mid组的方法状态(监控状态----->valueState---->记录访问日期)
                    private ValueState<String> theMidFirstVisitData; //当前设备第一次访问的时间
                    //定义日期格式化对象
                    private SimpleDateFormat sdf;

                    //open做初始化. 就相当于进来执行这个map算子的时候,来执行一次.到close的时候才做关闭.
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //只有跑起来之后才有RuntimeContext,所以放在open里面.
                        //对状态,以及日期格式做初始化.如何做初始化呢? 通过getRuntimeContext方法
                        theMidFirstVisitData = getRuntimeContext().getState(                    //新设备首次访问的类型
                                new ValueStateDescriptor<String>("newMidDataState", String.class)
                        );
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    //上面状态定义好了之后,下面是对数据进行处理了. 每条数据过来都要执行一次这个map.这里可以接收json对象.就是一条json对象日志.
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        //获取当前日志访问时间戳
                        Long ts = jsonObject.getLong("ts");
                        String currentDate = sdf.format(new Date(ts));
                        //获取前端标记的是否是新访客的标记.
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");

                        //前端标记新访客可能不准确.所以我需要对他的状态进行一个修复.
                        if ("1".equals(isNew)) {
                            //如何修复呢? 当前数据过来,需要看看我们状态中,这个mid以前是否过来过.做过记录.
                            //获取当前mid对应的状态的值.
                            String newMidDate = theMidFirstVisitData.value();//这个状态保存了你以前访问的日期.
                            //比较当前数据中状态日期和日志日期比较.
                            //判断当前记录的状态中是否有值.
                            if (newMidDate != null && newMidDate.length() != 0) {
                                //如果状态中有值.
                                //判断是否为同一天数据
                                if (!newMidDate.equals(currentDate)) {
                                    //如果不是同一天数据
                                    isNew = "0";
                                    //如果不是同一天的数据,说明is_new这个json数据中的这个是否是新用户值就是0,说明是新用户.
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                                //如果不是同一天的用户,就不需要更改.
                            } else {
                                //如果还没有记录状态,则将当前访问日期作为状态值
                                theMidFirstVisitData.update(currentDate);
                            }
                        }
                        return null;
                    }
                });
        //上一步是将is_new这个判断是否是新老客户的状态,前端可能误判,我们上面做了一个状态修复.
        //TODO 5 .分流  根据日志数据内容,将日志数据分为3类, 页面日志、启动日志和曝光日志。
        // 页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
        // 侧输出流作用： (1)接收迟到数据       (2)分流
        //创建启动日志的测输出流标签.
        //TODO 注意 这样写一个bug ,就是泛型擦除.  这样的话,虽然我泛型里面写的是String类型, 但是匿名函数给泛型擦除掉了, 不能这么写.
//        OutputTag<String> startOutPutTag = new OutputTag<>("start");
        //TODO 需要加个{} , 那么他会重写一个getTypeInfo这个方法
        OutputTag<String> startOutPutTag = new OutputTag<String>("start"){};
        //曝光侧输出流标签
        OutputTag<String> displayOutPutTag = new OutputTag<String>("display"){};


        //首先你要明白watermark是事件时间允许迟到数据,但是watermark3秒后迟到数据还没有来,我们还有allowLateness,但是如果allowLateness时间也过了,数据还没有来.我们也是需要考虑将这个数据处理掉
        //那就使用测数据流了.
        //即 watermark 迟到 ---> allowLateness迟到的迟到  ----->侧输出流处理迟到的迟到的迟到数据,进行保障. 这几层保障来处理迟到和乱序的数据.
        //为保证迟到的数据也能进入窗口我们需要设置AllowedLateness(Time),本质上就是:waterMark < (window-end+allowLateness)的数据进来都是会再次触发窗口计算
        //分流其实就是对数据做处理,但是你这是简简单单的map处理不太合适, 所以我们使用底层API 即processFunction API
        SingleOutputStreamOperator<String> pageProcessDataStream = jsonDataStreamWithFlag.process(
                //输入的是JSONObject,但是输出我们是放到kafka里面的,是String类型的.
                new ProcessFunction<JSONObject, String>() {
                    /*
                     processElement(v: IN, ctx: Context, out: Collector[OUT]), 流中的每一个元素都会调用这个方法，
                     调用结果将会放在Collector数据类型中输出。Context可以访问元素的时间戳，元素的key，以及TimerService时间服务。
                     Context还可以将结果输出到别的流(side outputs)。
                     v: IN------表示输入的数据,
                     v就是我要处理的元素,就是我要处理元素的类型,
                     ctx就是运行时的上下文环境,意思是RuntimeContext.
                     out是Collector类型,是收集器类型.
                     */
                    //TODO 流中的每一个元素都会调用这个方法，所以我需要分析数据的类型,是启动日志,曝光日志,页面日志等等.
                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                        //只有一条主流的情况下,你必须使用out:Collector[OUT]来输出.
                        //那如果你想要输出侧输出流呢?
                        //就是你还想搞一个分支,你还想把数据输出一个分支.输出到另外一个流里面去,输出到分支流里面去.那你就使用ctx: Context.

                        //获取启动日志数据,根据json数据的key来获取value的.
                        JSONObject startlogData = jsonObject.getJSONObject("start");
                        //将json类型转化为String类型
                        String LogdataString = startlogData.toJSONString();
                        //判断是否为启动日志
                        if (startlogData != null && startlogData.size() > 0) {
                            //说明key为start的json,获取的value值是有数据的.
                            //有数据就是启动日志,启动日志输出到启动的侧输出流.
                            context.output(startOutPutTag, LogdataString);
                        } else {
                            //TODO ①如果不是曝光日志,则是页面日志,页面日志输出到主流
                            collector.collect(LogdataString);
                            //如果不是启动日志,获取曝光日志标记
                            //因为里面的json对象是按照数组形式放置的. "display"[{...},{...}]
                            JSONArray displayLogData = jsonObject.getJSONArray("display");
                            //判断是否是曝光日志
                            if (displayLogData != null && displayLogData.size() > 0) {
                                //如果是曝光日志,输出侧输出流
                                //因为是数组,所以需要遍历
                                for (int i = 0; i < displayLogData.size(); i++) {
                                    //获取每一条曝光事件
                                    JSONObject displayJSONObject = displayLogData.getJSONObject(i);
                                    //这个不是输出的每个日志,而是输出json数组里面的每个事件
                                    //"display":[{...},{...}]  输出的是 {...} 这个,一个一个的输出出去.
                                    //而且,在曝光日志里面有个页面信息 {"display":[{...},{...}],"page":{...}}
                                    //key为page, page的value里面有page_id 页面信息. 我可以把当前的曝光事件在哪个页面曝光的,加上去
                                    //获取页面id
                                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                    //将页面id给每一条曝光事件display":[{...,pageId:pageid,...},{...,pageId:pageid,...}]}
                                    displayJSONObject.put("pageId", pageId);
                                    //侧输出流的输出
                                    context.output(displayOutPutTag, displayJSONObject.toString());
                                }
                            } /*else { //TODO 这是一个bug, 你应该在获取曝光日志之前直接将页面日志先输出过去.不然我们在后面分析UV的时候会出现没有数据的现象.因为曝光日志也携带页面
                                       //TODO 所以这一处应该放在①处
                                //不是曝光日志,则是页面日志,页面日志输出到主流
                                collector.collect(LogdataString);
                            }*/
                        }
                    }
                }
        );
        //获取测输出流
        DataStream<String> startSideOutput = pageProcessDataStream.getSideOutput(startOutPutTag);
        DataStream<String> displaySideOutput = pageProcessDataStream.getSideOutput(displayOutPutTag);

        //打印输出测试
        pageProcessDataStream.print("main主流");
        startSideOutput.print("start侧输出流");
        displaySideOutput.print("display侧输出流");

        //TODO 6.将不同流的数据写回到kafka的不同topic中
        //启动日志放启动主题
        FlinkKafkaProducer<String> startkafkaSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        startSideOutput.addSink(startkafkaSink);
        //曝光日志放曝光主题
        FlinkKafkaProducer<String> displayKafkaSink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);
        displaySideOutput.addSink(displayKafkaSink);
        //页面日志放到页面主题
        FlinkKafkaProducer<String> pagekafkaSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        pageProcessDataStream.addSink(pagekafkaSink);

        env.execute();
    }
}
