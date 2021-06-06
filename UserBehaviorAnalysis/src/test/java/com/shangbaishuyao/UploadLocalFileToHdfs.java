package com.shangbaishuyao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * ClassName:UploadLocalFileToHdfs <br/>
 * Function: 本地文件上传至hdfs. <br/>
 * Date:  2016年3月28日 下午10:06:05 <br/>
 * @author qiyongkang
 * @version
 * @since JDK 1.6
 * @see
 */
public class UploadLocalFileToHdfs {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String localDir = "H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\out\\2021-05-22--19";
        String hdfsDir = "/data";
        try{
            Path localPath = new Path(localDir);
            Path hdfsPath = new Path(hdfsDir);
            FileSystem hdfs = FileSystem.get(conf);
            hdfs.copyFromLocalFile(localPath, hdfsPath);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
