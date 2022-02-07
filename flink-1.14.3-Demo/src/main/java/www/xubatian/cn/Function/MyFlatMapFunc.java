package www.xubatian.cn.Function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author shangbaishuyao
 * @create 2022-01-21 下午3:13
 */

public class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>> {
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        //按照空格切分 hellospark...
        String[] words = value.split(" ");
        //遍历,相同字符标记为1
        for (String word: words){
            Tuple2<String, Integer> stringIntegerTuple2 = new Tuple2<String, Integer>(word, 1);
            out.collect(stringIntegerTuple2);
        }
    }
}