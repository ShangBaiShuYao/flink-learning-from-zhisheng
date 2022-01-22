//package www.xubatian.cn.FlinkDemo01;
//
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.operators.DataSource;
//import www.xubatian.cn.Function.MyFlatMapFunc;
//
///**
// * @author shangbaishuyao
// * @create 2022-01-21 下午3:02
// */
//
//public class Flink_WordCount_Batch {
//    public static void main(String[] args) throws Exception{
//        //获取执行环境
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        //读取文本文件
//        DataSource<String> dataSource = env.readTextFile("/Users/shangbaishuyao/warehouse/IDEA_WorkSpace/Flink_WorkSpace/flink-learning-from-zhisheng/flink-1.14.3-Demo/input/word.txt");
//
//        dataSource.flatMap();
//
//    }
//}
