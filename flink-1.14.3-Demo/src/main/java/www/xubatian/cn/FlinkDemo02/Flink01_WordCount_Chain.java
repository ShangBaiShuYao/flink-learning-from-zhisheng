package www.xubatian.cn.FlinkDemo02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author shangbaishuyao
 * @create 2022-02-10 下午9:14
 * @Desc: 任务链 <br/>
 * 任务链形成的三条件:
 * ①并行度相同
 * ②算子间没有shuffle
 * ③相同的共享组
 */

public class Flink01_WordCount_Chain {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //全局禁用任务链
        //env.disableOperatorChaining();
        //读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //数据转换为元组类型
        socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String socketTextStream, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按照空格拆分每条数据
                String[] wordArray = socketTextStream.split(" ");
                //将每个单词转变为元组类型
                for (String word : wordArray){
                    //输出元组类型 ("word",1)
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    out.collect(tuple2);
                }
            }
        })/*.disableChaining();*/
        //分组
        .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> wordTuple2) throws Exception {
                //获取 word
                String Tuple2Key = wordTuple2.f0;
                return Tuple2Key;
            }
        })
        //按照 key 计算. 相同key相加
        .sum(1)
        //打印
        .print();
        //启动任务
        env.execute("任务链案例");
    }
}
