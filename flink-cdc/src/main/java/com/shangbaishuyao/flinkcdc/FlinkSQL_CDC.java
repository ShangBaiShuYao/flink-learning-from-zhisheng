package com.shangbaishuyao.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Author: shangbaishuyao
 * Date: 11:58 2021/5/16
 * Desc: FlinkSQL方式的应用Flink CDC
 *
 * 前提: mysql对这个库的这张表开启binlog.
 *
 * TODO 只要true 和 false及true都要的两种情况的优缺点是什么呢? 格式很恶心. 所以我们就有了自定义反序列化器.
 *
 */
public class FlinkSQL_CDC {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.创建Flink-MySQL-CDC的Source
        tableEnv.executeSql("CREATE TABLE user_info (" +
                "  id INT," +
                "  name STRING," +
                "  phone_num STRING" +
                ") WITH (" +
                "  'connector' = 'mysql-cdc'," +
                "  'hostname' = 'hadoop102'," +
                "  'port' = '3306'," +
                "  'username' = 'root'," +
                "  'password' = 'xww2018'," +
                "  'database-name' = 'shangbaishuyao'," +
                "  'table-name' = 'z_user_info'" +
                ")");

        //查询表数据(只要前面为true的)
        tableEnv.executeSql("select * from user_info").print();

        //测试需要哪些行为的数据 (查看哪些操作会出现false和true的现象,发现. 修改一个记录会出现false和true两条记录. 删除则会出现false记录)
        Table table = tableEnv.sqlQuery("select * from user_info");
        tableEnv.toRetractStream(table, Row.class).print(); //toRetractStream: 因为有更新和删除,所以使用撤回流.而不是使用追加流
        //如果修改原本数据库中的内容就会出现两条打印: 一个是false,一个是true.  如果是删掉一条记录,测试false.

        //开启任务
        env.execute();
    }
}
