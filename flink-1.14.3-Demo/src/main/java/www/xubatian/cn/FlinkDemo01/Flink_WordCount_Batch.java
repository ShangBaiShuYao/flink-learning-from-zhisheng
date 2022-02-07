package www.xubatian.cn.FlinkDemo01;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import www.xubatian.cn.Function.MyFlatMapFunc;

/*
 * @author: shangbaishuyao
 * @des:
 * @date: 下午3:29 2022/1/31
 **/
public class Flink_WordCount_Batch {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataSource<String> lineDataStream = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.14.3-Demo/input/word.txt");
        //装换数据格式,groupby分组, sum聚合
        AggregateOperator<Tuple2<String, Integer>> wordCountDataSet = lineDataStream.flatMap(new MyFlatMapFunc())
                .groupBy(0)
                .sum(1);
        //打印输出
        wordCountDataSet.print();
    }
}
