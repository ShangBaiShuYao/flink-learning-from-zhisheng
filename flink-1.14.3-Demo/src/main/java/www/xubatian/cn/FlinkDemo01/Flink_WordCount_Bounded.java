package www.xubatian.cn.FlinkDemo01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import www.xubatian.cn.Function.MyFlatMapFunc3;

/**
 * @author shangbaishuyao
 * @create 2022-02-07 下午2:15
 * Desc: 有界流 案例<br/>
 */

public class Flink_WordCount_Bounded {
    public static void main(String[] args) throws Exception{
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取文件创建流
        DataStreamSource<String> input = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.14.3-Demo/input/word.txt");
        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordsTuple2 = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //按照空格切分
                String[] wordsArray = value.split(" ");
                //批量输出
                for (String word : wordsArray) {
                    out.collect(word);
                }
            }
            //4.将单词转换为2元组
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
        //在批处理中使用的是 groupby(), 但是在流里面没有,使用的是keyBy()
        //5.元组的索引是0开始, 所以按照0分组
        wordsTuple2.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0; //相当于cala中的 ._1
            }
            //6. 聚合         7.打印结果
        }).sum(1).print();
        //8. 启动流式程序
        env.execute("Flink_WordCount_Bounded");
    }
}
