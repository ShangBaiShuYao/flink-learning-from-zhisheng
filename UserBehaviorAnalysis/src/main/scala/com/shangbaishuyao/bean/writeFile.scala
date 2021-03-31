package com.shangbaishuyao.bean

import java.io.RandomAccessFile
import java.nio.charset.Charset

import scala.io.Source
import scala.reflect.io.Path

class writeFile {
  object FileOpera {
    val file_path = "H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\hdfs.txt"; //生成文件数据路径
    //    val file_path="D:\\tmp\\csv\\1";//生成文件数据路径
    val path: Path = Path(file_path) //得到一个Path
    if (path.exists) { //判断是否存在
      path.deleteRecursively() //递归删除，无论是目录还是文件
      println("存在此文件，已经删除!")
    } else {
      println("文件不存在,开始生成.....!")
    }
    val raf = new RandomAccessFile(file_path, "rw") //组合java的流
    val file = Source.fromFile("H:\\IDEA_WorkSpace\\flink-learning-from-zhisheng\\UserBehaviorAnalysis\\Data\\hdfs.txt"); //读取一个文件
    for (line <- file.getLines()) { //遍历每一行内容
      val num = line.trim.split(",").length; //根据逗号拆分后的数组长度
      if (num == 1) {
        //          println(line);
      } else {
        if (line.trim.endsWith(",")) {
          val data = new StringBuilder(line.trim).deleteCharAt(line.trim.length - 1).append("\n").toString().replace("＃", "");
          raf.write(data.getBytes(Charset.forName("UTF-8")))
        }
      }
    }

    //读取流关闭
    file.close();
    //写入流关闭
    raf.close();
  }
}
