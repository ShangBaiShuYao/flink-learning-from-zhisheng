package com.shangbaishuyao.Handler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Desc: 删除HDFS <br/>
 * create by shangbaishuyao on 2021/3/31
 * @Author: 上白书妖
 * @Date: 18:55 2021/3/31
 */
public class writeHDFS {
    public static void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "shangbaishuyao");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\hdfs.txt"), new Path("/data/"));
        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }

    public static void main(String[] args) {
        try {
            writeHDFS.testCopyFromLocalFile();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
