package com.shangbaishuyao.sources.sources;

import com.shangbaishuyao.sources.model.Student;
import com.shangbaishuyao.sources.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Desc: 自定义source,从mysql中读取数据<br/>
 * create by shangbaishuyao on 2020-12-11
 *@Author: 上白书妖
 *@Date: 2020/12/11 13:56
 */
public class SourceFromMySQL  extends RichSourceFunction<Student> {
    PreparedStatement ps;
    private Connection connection;

    /**
     * Desc: 在open方法中建立连接, 这样不用每次invok的时候都要建立连接和释放连接<br/>
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtil.getConnection(null, "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root");
        String sql = "select * from Student";
        ps = connection.prepareStatement(sql);
    }


    /**
     * Desc: DataStream调用一次run() 方法来获取数据<br/>
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            Student student = new Student(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));
            ctx.collect(student);
        }
    }

    /**
     * Desc: 程序执行完毕之后就可以进行关闭连接,释放资源<br/>
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接
        if (connection!=null){
            connection.close();
        }
        if (ps != null){
            ps.close();
        }
    }

    @Override
    public void cancel() {
    }
}
