package www.xubatian.cn.FlinkDemo01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



/**
 * @author shangbaishuyao
 * @create 2022-02-10 上午9:45
 */

public class Flink_WordCount_UnBounded {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为2
        env.setParallelism(2);
        //读取端口数据
        DataStreamSource<String> sourceDataStream = env.socketTextStream("hadoop102", 9999);
        //数据转换
        sourceDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word,1); //("word",1)
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0; //("word",1) = value     value.f0 = word
            }
        }).sum(1) //按照word进行聚合 相同的加1
                .print();
        env.execute("读取socket源数据形成无界流");
    }
}
