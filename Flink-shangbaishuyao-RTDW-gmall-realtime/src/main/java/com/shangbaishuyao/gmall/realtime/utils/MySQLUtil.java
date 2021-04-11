package com.shangbaishuyao.gmall.realtime.utils;

import com.shangbaishuyao.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc: 从MySQL数据中查询数据的工具类 <br/>
 * 完成ORM，对象关系映射
 * O：Object对象       Java中对象
 * R：Relation关系     关系型数据库
 * M:Mapping映射      将Java中的对象和关系型数据库的表中的记录建立起映射关系
 * 数据库                 Java
 * 表t_student           类Student
 * 字段id，name           属性id，name
 * 记录 100，zs           对象100，zs
 *
 * 从数据库中查询出来是在ResultSet(一条条记录) 这个结果集里面 ,然后我们将他封装在List(一个个Java对象)集合中.
 * @Author: 上白书妖
 * @Date: 23:22 2021/4/10
 *
 */
public class MySQLUtil {
    /**
     * @param sql               执行的查询语句
     * @param clz               返回的数据类型
     * @param underScoreToCamel 是否将下划线转换为驼峰命名法
     * @param <T>
     * @return
     */
    //掉用我方法的时候,传一个类型过来.
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //创建连接
            conn = DriverManager.getConnection(
                "jdbc:mysql://hadoop102:3306/gmall_realtime?characterEncoding=utf-8&useSSL=false",
                "root",
                "shangbaishuyao");
            //创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            // 100      zs      20
            // 200		ls 		30
            rs = ps.executeQuery();
            //处理结果集
            //查询结果的元数据信息,元数据信息里面有啥我也不知道,你可以自己调用看看,有列的名称,列的数量
            // id		student_name	age
            ResultSetMetaData metaData = rs.getMetaData();
            List<T> resultList = new ArrayList<T>();
            //判断结果集中是否存在数据，如果有，那么进行一次循环
            while (rs.next()) {
                //创建一个对象，用于封装查询出来一条结果集中的数据
                //这个对象不能写死,不能只是student对象, 你还要从品牌表,品类表等等表中获取数据,所以用下面的这个
                //使用clazz.newInstance()来实例化一个对象出来. T是和你方法传的类型一样. 因为我现在并不确定.
                T obj = clz.newInstance();
                //对查询的所有列进行遍历，获取每一列的名称
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    String propertyName = columnName;
                    if(underScoreToCamel){
                        //谷歌的guva方法,将下划线转化为驼峰命名法
                        //如果指定将下划线转换为驼峰命名法的值为 true，通过guava工具类，将表中的列转换为类属性的驼峰命名法的形式
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //原来对属性赋值是get.set方法,而现在你连对象是谁都不知道,怎么办呢?
                    //咱们可以结束commons-bean工具类,他有一个方法是给属性赋值的,叫setProperty.
                    //调用apache的commons-bean中工具类，给obj属性赋值
                    BeanUtils.setProperty(obj,propertyName,rs.getObject(i));
                }
                //将一条条数据封装好的对象放到List集合中.
                //将当前结果中的一行数据封装的obj对象放到list集合中
                resultList.add(obj);
            }
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从MySQL查询数据失败");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        List<TableProcess> list = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : list) {
            System.out.println(tableProcess);
        }
    }
}
/*
<p>需要引入的依赖<p/>
<!--lomback插件依赖-->
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <version>1.18.12</version>
    <scope>provided</scope>
</dependency>
<!--commons-beanutils是Apache开源组织提供的用于操作JAVA BEAN的工具包。
使用commons-beanutils，我们可以很方便的对bean对象的属性进行操作-->
<dependency>
    <groupId>commons-beanutils</groupId>
    <artifactId>commons-beanutils</artifactId>
    <version>1.9.3</version>
</dependency>
<!--Guava工程包含了若干被Google的Java项目广泛依赖的核心库,方便开发-->
<dependency>
    <groupId>com.google.guava</groupId>
    <artifactId>guava</artifactId>
    <version>29.0-jre</version>
</dependency>

<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.47</version>
</dependency>
 */