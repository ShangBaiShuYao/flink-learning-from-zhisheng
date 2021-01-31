package com.shangbaishuyao.connectors.mysql.sinks;

import com.shangbaishuyao.connectors.mysql.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Desc: 数据批量 sink 数据到 mysql <br/>
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 15:14 2021/1/31
 */
@Slf4j
public class SinkToMySQL extends RichSinkFunction<List<Student>> {
    BasicDataSource basicDataSource;
    PreparedStatement ps;
    Connection connection;
    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //初始化基本数据源用于连接dbcp2
        basicDataSource = new BasicDataSource();
        connection = getConnection(basicDataSource);
        String sql = "insert into Student ";

    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Student> value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
        //遍历数据集合
        for (Student student : value) {
            ps.setInt(1, student.getId());
            ps.setString(2, student.getName());
            ps.setString(3, student.getPassword());
            ps.setInt(4, student.getAge());
            ps.addBatch();
        }
        int[] count = ps.executeBatch();//批量后执行
        log.info("The {} row was inserted successfully / 成功了插入了 {} 行数据", count.length);
    }


    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放连接
        if (connection == null){
            connection.close();
        }

        if (ps == null){
            ps.close();
        }
    }

    //数据源连接
    private static Connection getConnection(BasicDataSource dataSource) {
        /**
         * 基本数据原中设置参数
         */
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://localhost:3306/test");
        dataSource.setUsername("root");
        dataSource.setPassword("root");
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
//            con = dataSource.getConnection();
            dataSource.getConnection();
            log.info("创建连接池：{}", con);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }

}
