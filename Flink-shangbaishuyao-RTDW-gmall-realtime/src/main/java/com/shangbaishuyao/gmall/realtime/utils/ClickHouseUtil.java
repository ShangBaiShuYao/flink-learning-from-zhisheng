package com.shangbaishuyao.gmall.realtime.utils;

import com.shangbaishuyao.gmall.realtime.bean.TransientSink;
import com.shangbaishuyao.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Author: 上白书妖
 * Date: 2021/2/23
 * Desc: 操作ClickHouse的工具类
 */
public class ClickHouseUtil {
    /**
     * 获取向Clickhouse中写入数据的SinkFunction
     *
     * @param sql
     * @param <T>
     * @return
     */
    public static <T> SinkFunction getJdbcSink(String sql) {
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
            //要执行的SQL语句
            sql,
            //执行写入操作   就是将当前流中的对象属性赋值给SQL的占位符 insert into visitor_stats_0820 values(?,?,?,?,?,?,?,?,?,?,?,?)
            new JdbcStatementBuilder<T>() {
                //obj  就是流中的一条数据对象
                @Override
                public void accept(PreparedStatement ps, T obj) throws SQLException {
                    //获取当前类中  所有的属性
                    Field[] fields = obj.getClass().getDeclaredFields();
                    //跳过的属性计数
                    int skipOffset = 0;

                    for (int i = 0; i < fields.length; i++) {
                        Field field = fields[i];
                        //通过属性对象获取属性上是否有@TransientSink注解
                        TransientSink transientSink = field.getAnnotation(TransientSink.class);
                        //如果transientSink不为空，说明属性上有@TransientSink标记，那么在给?占位符赋值的时候，应该跳过当前属性
                        if (transientSink != null) {
                            skipOffset++;
                            continue;
                        }

                        //设置私有属性可访问
                        field.setAccessible(true);
                        try {
                            //获取属性值
                            Object o = field.get(obj);
                            ps.setObject(i + 1 - skipOffset, o);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                }
            },
            //构建者设计模式，创建JdbcExecutionOptions对象，给batchSize属性赋值，执行执行批次大小
            new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
            //构建者设计模式，JdbcConnectionOptions，给连接相关的属性进行赋值
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(GmallConfig.CLICKHOUSE_URL)
                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                .build()
        );
        return sinkFunction;
    }
}
