package com.shangbaishuyao.sources.utils;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Desc:MySQL工具类<br/>
 * create by shangbaishuyao on 2020-12-11
 *@Author: 上白书妖
 *@Date: 2020/12/11 14:01
 */
public class MySQLUtil {
    public static Connection getConnection(String driver, String url,String user,String password){
        Connection connection  = null;
        try {
            if (driver == null || StringUtils.isBlank(driver)){
                Class.forName("com.mysql.jdbc.Driver");
            }else {
                Class.forName(driver);
            }
           connection = DriverManager.getConnection(url, user, password);
        }catch (Exception e){
            System.out.println("mysql get connection has exception , msg = " + e.getMessage());
        }
        return connection;
    }
}
