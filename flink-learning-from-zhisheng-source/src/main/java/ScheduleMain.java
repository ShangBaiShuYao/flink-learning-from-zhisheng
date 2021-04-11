import com.shangbaishuyao.common.model.MetricEvent;
import com.shangbaishuyao.common.utils.ExecutionEnvUtil;
import com.shangbaishuyao.common.utils.KafkaConfigUtil;
import com.shangbaishuyao.sources.model.Rule;
import com.shangbaishuyao.sources.utils.MySQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Desc: 定时捞取告警规则<br/>
 * create by shangbaishuyao on 2020-12-11
 *@Author: 上白书妖
 *@Date: 2020/12/11 16:23
 */
@Slf4j
public class ScheduleMain {

    public static  List<Rule> rules;

    public static void main(String[] args) throws Exception {
        //定时捞取规则,每隔一分钟捞取一次 使用线程池
        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(1);
        threadPool.scheduleAtFixedRate(new GetRulesJob(), 0,1, TimeUnit.MILLISECONDS );

        //获取你的配置值并传入ParameterTool中
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        //ParameterTool值都获取到后赋值流式环境变量中
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        //构建数据源
        DataStreamSource<MetricEvent> source = KafkaConfigUtil.buildSource(env);

        //Flink中每一个算子,他都给你提供了一个函数对象作为参数
        source.map(new MapFunction<MetricEvent, MetricEvent>() {
            @Override
            public MetricEvent map(MetricEvent metricEvent) throws Exception {
                //Flink暴露了所有udf函数的接口(实现方式为接口或者抽象类)。
                // 例如MapFunction做转换的, FilterFunction做过滤的,
                // ProcessFunction不知道做转换,做过滤还是做其他的,
                // 但是我肯定要处理数据,则使用processFunction,他是没有限制的,
                // 你想做什么你就在这里面写什么就好了等等

                if (rules.size()<=2){
                    System.out.println("===============2");
                }else {
                    System.out.println("===============3");
                }

                return metricEvent;
            }
        }).print();
        env.execute("schedule");
    }


    //得到规则job
   public static class GetRulesJob implements Runnable{

       @Override
       public void run() {
            try {
                 rules = getRules();
            }catch (Exception e) {
                log.error("从mysql获取规则有一个错误{} / get rules from mysql has an error{}",e.getMessage());
            }
       }
   }


   //得到规则,使用实体类封装数据
   public static List<Rule> getRules() throws SQLException{
       System.out.println("========get rule========");
       String sql = "select * from rule";

       //连接数据库
       Connection connection = MySQLUtil.getConnection(
               "com.mysql.jdbc.Driver",
               "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8",
               "root",
               "root");

       PreparedStatement ps = connection.prepareStatement(sql);
       //执行查询语句
       ResultSet resultSet = ps.executeQuery();


       List<Rule> list = new ArrayList<>();

       //将数据库中查到的数据赋值给对象的对应字段 这里用到的是@build注解
       while (resultSet.next()){
            list.add(Rule.builder()
                    .id(resultSet.getString("id"))
                    .name(resultSet.getString("name"))
                    .type(resultSet.getString("type"))
                    .measurement(resultSet.getString("measurement"))
                    .threshold(resultSet.getString("threshold"))
                    .level(resultSet.getString("level"))
                    .targetType(resultSet.getString("target_type"))
                    .targetId(resultSet.getString("target_id"))
                    .webhook(resultSet.getString("webhook"))
                    .build()
            );
       }

       return list;
   }
}
