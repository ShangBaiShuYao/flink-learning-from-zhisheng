import com.shangbaishuyao.common.utils.ExecutionEnvUtil;
import com.shangbaishuyao.sources.model.Rule;
import com.shangbaishuyao.sources.utils.MySQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

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
 * create by shangbaishuyao 2020-12-11
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

        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
    }


    //得到规则job
   public static class GetRulesJob implements Runnable{

       @Override
       public void run() {
            try {
                 rules = getRules();
            }catch (Exception e) {
                log.error("从mysql获取规则有一个错误{}",e.getMessage());
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
