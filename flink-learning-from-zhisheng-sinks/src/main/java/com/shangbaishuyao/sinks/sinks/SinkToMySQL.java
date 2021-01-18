package com.shangbaishuyao.sinks.sinks;

import com.shangbaishuyao.sinks.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Desc:sink 数据到mysql <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/25 9:07
 */
@Slf4j
public class SinkToMySQL extends RichSinkFunction<Student> {

    PreparedStatement preparedStatement;
    private Connection connection;

    //①使用jdbc连接mysql
    private static Connection getConnection(){
        Connection cn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            cn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root");
        }catch(Exception e) {
            log.error("=================================mysql get Connection has Exception , msg= {}",e.getMessage());
        }
        return cn;
    }


    /**
     * ②在open方法这种建立连接,这样不用每次invoke的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //调用上面写的使用jdbc连接mysql数据库的方法
        connection = getConnection();
        //插入的sql语句
        String sql = "insert into Student(id, name, password, age) values(?, ?, ?, ?);";
        if (connection != null){
             preparedStatement = this.connection.prepareStatement(sql);
        }
    }

    /**
     * ④关闭
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null){
            connection.close();
        }
        if (preparedStatement != null){
            preparedStatement.close();
        }
    }

    /**
     * ③每条数据的插入都要调用一次 invoke() 方法
     *
     * @param student
     * @param context
     * @throws Exception
     */
    //调用连接
    @Override
    public void invoke(Student student, Context context) throws Exception {
        if (preparedStatement != null){
            return;
        }
        preparedStatement.setInt(1,student.getId());
        preparedStatement.setString(2,student.getName());
        preparedStatement.setString(3,student.getPassword());
        preparedStatement.setInt(4,student.getAge());
        //执行跟新方法
        preparedStatement.executeUpdate();
    }

}
