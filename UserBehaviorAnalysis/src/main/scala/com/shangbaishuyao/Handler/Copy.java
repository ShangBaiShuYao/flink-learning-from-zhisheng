package com.shangbaishuyao.Handler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import java.io.*;
import java.net.URI;

/**
 * 首先在网上找了好久没有找到从本地文件系统上传整个文件夹到hdfs文件系统的程序，权威指南上也没有，都是单个文件上传，
 * 所以这里自己编写了一个程序，封装成jar包运行可以复制。
 * 先说明一下代码：需要手动输入两个路径，一个本地文件/文件夹路径，第二个是hdfs文件夹路径。好直接上代码：
 *
 */
public class Copy {
    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            System.out.println("Please input two number");
            System.exit(2);
        }
        String localSrc = args[0];
        String dst = args[1];
        Configuration conf = new Configuration();
        File srcFile = new File(localSrc);
        if(srcFile.isDirectory()){
            copyDirectory(localSrc , dst , conf);
        }else{
            copyFile(localSrc, dst, conf);
        }
    }

    public static void test(String url1 ,String url2) throws Exception {
        String localSrc = url1;
        String dst = url2;
        Configuration conf = new Configuration();
        File srcFile = new File(localSrc);
        if(srcFile.isDirectory()){
            copyDirectory(localSrc , dst , conf);
        }else{
            copyFile(localSrc, dst, conf);
        }
    }

    //上传本地文件
    public static void putTxt() throws Exception {
        /**
         * Configuration参数对象的机制：
         *    构造时，会加载jar包中的默认配置 xx-default.xml
         *    再加载 用户配置xx-site.xml  ，覆盖掉默认参数
         *    构造完成之后，还可以conf.set("p","v")，会再次覆盖用户配置文件中的参数值
         */
        // new Configuration()会从项目的classpath中加载core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml等文件
        Configuration conf = new Configuration();

        // 指定本客户端上传文件到hdfs时需要保存的副本数为：2
        conf.set("dfs.replication", "2");
        // 指定本客户端上传文件到hdfs时切块的规格大小：64M
        conf.set("dfs.blocksize", "64m");

        // 构造一个访问指定HDFS系统的客户端对象: 参数1:——HDFS系统的URI，参数2：——客户端要特别指定的参数，参数3：客户端的身份（用户名）
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000/"), conf, "shangbaishuyao");

        // 上传一个文件到HDFS中
        fs.copyFromLocalFile(new Path("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\out\\20210523134425284.part-0-0\\2021-05-23--14\\.part-0-0.inprogress.49a8be85-b24f-4247-85b1-2f18be90939d"), new Path("/data/"));

        fs.close();
    }


    /**
     * 拷贝文件
     * @param src
     * @param dst
     * @param conf
     * @return
     * @throws Exception
     */
    public static boolean copyFile(String src , String dst , Configuration conf) throws Exception{
        FileSystem fs = FileSystem.get(conf);
        fs.exists(new Path(dst));
        //FileStatus status = fs.getFileStatus(new Path(dst));
        File file = new File(src);

        InputStream in = new BufferedInputStream(new FileInputStream(file));
        /**
         * FieSystem的create方法可以为文件不存在的父目录进行创建，
         */
        OutputStream out = fs.create(new Path(dst) , new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);

        return true;
    }
    /**
     * 拷贝文件夹
     * @param src
     * @param dst
     * @param conf
     * @return
     * @throws Exception
     */
    public static boolean copyDirectory(String src , String dst , Configuration conf) throws Exception{

        FileSystem fs = FileSystem.get(conf);
        if(!fs.exists(new Path(dst))){
            fs.mkdirs(new Path(dst));
        }
        System.out.println("copyDirectory:"+dst);
        FileStatus status = fs.getFileStatus(new Path(dst));
        File file = new File(src);

        if(status.isFile()){
            System.exit(2);
            System.out.println("You put in the "+dst + "is file !");
        }else{
            dst = cutDir(dst);
        }
        File[] files = file.listFiles();
        for(int i = 0 ;i< files.length; i ++){
            File f = files[i];
            if(f.isDirectory()){
                copyDirectory(f.getPath(),dst,conf);
            }else{
                copyFile(f.getPath(),dst+files[i].getName(),conf);
            }
        }
        return true;
    }
    public static String cutDir(String str){
        String[] strs = str.split(File.pathSeparator);
        String result = "";
        if("hdfs"==strs[0]){
            result += "hdfs://";
            for(int i = 1 ; i < strs.length  ; i++){
                result += strs[i] + File.separator;
            }
        }else{
            for(int i = 0 ; i < strs.length  ; i++){
                result += strs[i] + File.separator;
            }
        }
        return result;
    }
}
