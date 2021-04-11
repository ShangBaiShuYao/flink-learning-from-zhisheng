package com.shangbaishuyao.gmall.realtime.app.Function;

import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmall.realtime.common.GmallConfig;
import com.shangbaishuyao.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * Desc: 写出维度数据的Sink实现类 <br/>
 * 将维度数据,使用Phoenix写入到Hbase中.
 * @Author: 上白书妖
 * @Date: 16:50 2021/4/11
 */
public class PhoenixSinkFunction extends RichSinkFunction<JSONObject> {
    //定义Phoenix连接对象
    private Connection conn = null;

    //程序运行时调用一次
    //做一次连接操作
    @Override
    public void open(Configuration parameters) throws Exception {
        //对连接对象进行初始化
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * invoke方法是什么时候调用? 每过来一个元素,我的invoke方法都要调用一次.
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取目标表的名称
        String tableName = value.getString("sink_table");
        //获取json中data数据   data数据就是经过过滤之后  保留的业务表中字段
        JSONObject dataJsonObj = value.getJSONObject("data");

        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            //根据data中属性名和属性值  生成upsert语句
            String upsertSql = genUpsertSql(tableName.toUpperCase(), dataJsonObj);
            System.out.println("向Phoenix插入数据的SQL:" + upsertSql);

            //执行SQL
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(upsertSql);
                //TODO 注意: execute和executeUpdate主要是做DML操作的,DML操作主要是crud ; DDL操作主要是创建和操作表结构;
                //execute和executeUpdate区别主要是返回值类型
                ps.execute();
                //TODO 注意：执行完Phoenix插入操作之后，需要手动提交事务
                //这里为什么要手动提交事务呢? 不管是mysql还是Phoenix都遵循了JDBC协议,JDBC协议里面有个connection接口,你在Phoenix要实现这个接口,在mysql中也要实现这个接口
                //在这个Mysqlconnection中有个方法叫setAutoCommit.这个是事务提交的方法. 在mysql中是自动帮你提交的
                //但是Phoeniconnection中也有这个setAutoCommit方法,但是这方法默认不自动提交. 所以你需要手动提交事务.
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("向Phoenix插入数据失败");
            } finally {
                if (ps != null) {
                    ps.close();
                }
            }
            //如果当前做的是更新操作，需要将Redis中缓存的数据清除掉
            if(value.getString("type").equals("update")){
                DimUtil.deleteCached(tableName,dataJsonObj.getString("id"));
            }
        }
    }
    // 根据data属性和值  生成向Phoenix中插入数据的sql语句
    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        /*
            {
                "id":88,
                "tm_name":"xiaomi"
            }
        */
        //想要获取map集合key的部分
        Set<String> keys = dataJsonObj.keySet();
        //获取map集合的value值
        Collection<Object> values = dataJsonObj.values();
        //upsert into 表空间.表名(列名.....) values (值....)
        String upsertSql = "upsert into " + GmallConfig.HABSE_SCHEMA + "." + tableName + "(" + StringUtils.join(keys, ",") + ")";
        //只用逗号,将两个列连接在一起.(逗号连接集合中的元素)
        //在apche Common的lang包下有一个StringUtils.join  这个join就是使用指定的字符去连接现在集合中的元素.
        String valueSql = " values ('" + StringUtils.join(values, "','") + "')"; //'A','B','C' 这才是值的形式
        return upsertSql + valueSql;
    }
}
