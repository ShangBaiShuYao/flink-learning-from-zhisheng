package www.xubatian.cn.FlinkDemo01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/*
 * @author: shangbaishuyao
 * @des: 批处理案例 <br/>
 * @date: 下午3:29 2022/1/31
 **/
public class Flink_WordCount_Batch {
    public static void main(String[] args) throws Exception{
        //创建 批处理 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataSource<String> lineDataStream = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.14.3-Demo/input/word.txt");
        //压平("1",1) ("2",1) ("3",1) ("4",1)
//        FlatMapOperator<String, Tuple2<String, Integer>> tuple2FlatMapOperator = lineDataStream.flatMap(new MyFlatMapFunc3());
        //也可以直接使用map将单词转换为元组
       lineDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] wordsArray = value.split(" ");
                for (String word : wordsArray) {
                    out.collect(word);
                }
            }
            //获取元组操作
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value, 1);
            }
            //元组分组         聚合         打印
        }).groupBy(0).sum(1).print();
    }
}
