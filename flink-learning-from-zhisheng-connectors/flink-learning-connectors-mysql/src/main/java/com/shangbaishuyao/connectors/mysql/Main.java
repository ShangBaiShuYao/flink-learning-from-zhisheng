package com.shangbaishuyao.connectors.mysql;

import com.google.common.collect.Lists;
import com.shangbaishuyao.common.constant.PropertiesConstants;
import com.shangbaishuyao.common.utils.ExecutionEnvUtil;
import com.shangbaishuyao.common.utils.GsonUtil;
import com.shangbaishuyao.common.utils.KafkaConfigUtil;
import com.shangbaishuyao.connectors.mysql.model.Student;
import com.shangbaishuyao.connectors.mysql.sinks.SinkToMySQL;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/*
 * Desc:
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 14:50 2021/1/31
 */
@Slf4j
public class Main {
    public static void main(String[] args) throws Exception{
        final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                PropertiesConstants.METRICS_TOPIC,  //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(), //创建一个使用“UTF-8”作为编码的新的SimpleStringSchema。
                properties //配置
                //设置并行度
        )).setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 1))
                //解析字符串成 student 对象
                .map(string -> GsonUtil.fromJson(string, Student.class))
                .setParallelism(4);//设置并行度


        //TimeWindow是将指定时间范围内的所有数据组成一个window，一次对一个window里面的所有数据进行计算。
        //timeWindow函数必须在keyBy之后，timeWindowAll则不需要
        //timeWindowAll 并行度只能为 1
        student.timeWindowAll(Time.minutes(1))
                .apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Student> values, Collector<List<Student>> out) throws Exception {
                        /**
                         * lists.newarraylist()：
                         * List<String> list = new ArrayList<String>();
                         *
                         * new arraylist() ：
                         * List<String> list = Lists.newArrayList();
                         *
                         * Lists和Maps是两个工具类,
                         * Lists.newArrayList()其实和new ArrayList()几乎一模一样,
                         * 唯一它帮你做的(其实是javac帮你做的),
                         * 就是自动推导尖括号里的数据类型.
                         * 在 Java 7 之后，都允许类型推断 - 在运行时没有区别。
                         * 一它帮你做的(其实是javac帮你做的), 就是自动推导(不是"倒")尖括号里的数据类型.
                         */
                        ArrayList<Student> students = Lists.newArrayList(values);

                        if (students.size() > 0 ){
                            log.info("1 分钟内收集到 student 的数据条数是：" + students.size());
                            out.collect(students);
                        }
                    }
                }).addSink(new SinkToMySQL()).setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_SINK_PARALLELISM, 1));

                env.execute("flink learning connnectors mysql");
    }
}
