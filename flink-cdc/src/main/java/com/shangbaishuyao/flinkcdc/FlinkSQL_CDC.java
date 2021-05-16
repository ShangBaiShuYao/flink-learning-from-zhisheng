package com.shangbaishuyao.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: shangbaishuyao
 * Date: 11:58 2021/5/16
 * Desc: FlinkSQL方式的应用Flink CDC
 *
 * 前提: mysql对这个库的这张表开启binlog.
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

        tableEnv.executeSql("select * from user_info").print();
        env.execute();
    }
}
