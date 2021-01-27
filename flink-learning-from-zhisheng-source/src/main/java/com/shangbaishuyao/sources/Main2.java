package com.shangbaishuyao.sources;

import com.shangbaishuyao.sources.sources.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc:自定义source 从mysql中读取数据<br/>
 * create by shangbaishuyao on 2020-12-11
 *@Author: 上白书妖
 *@Date: 2020/12/11 16:10
 */
public class Main2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SourceFromMySQL sourceFromMySQL = new SourceFromMySQL();

        env.addSource(sourceFromMySQL).print();

        env.execute("Flink add data source");
    }
}
