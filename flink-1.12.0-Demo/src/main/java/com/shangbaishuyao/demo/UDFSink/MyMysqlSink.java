package com.shangbaishuyao.demo.UDFSink;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
//写入mysql
public class MyMysqlSink extends RichSinkFunction<WaterSensor> {
    //声明连接
    private Connection connection;
    private PreparedStatement preparedStatement;
    //生命周期方法,用于创建连接
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(
                "jdbc:mysql://hadoop102:3306/test?useSSL=false",
                "root", "xww2018");
        preparedStatement = connection.prepareStatement(
                "INSERT INTO `sensor-0821` VALUES(?,?,?) ON DUPLICATE KEY UPDATE `ts`=?,`vc`=?");
    }

    @Override
    public void invoke(WaterSensor value, SinkFunction.Context context) throws Exception {

        //给占位符赋值
        preparedStatement.setString(1, value.getId());
        preparedStatement.setLong(2, value.getTs());
        preparedStatement.setInt(3, value.getVc());
        preparedStatement.setLong(4, value.getTs());
        preparedStatement.setInt(5, value.getVc());

        //执行操作
        preparedStatement.execute();

    }
    //生命周期方法,用于关闭连接
    @Override
    public void close() throws Exception {
        preparedStatement.close();
        connection.close();
    }
}