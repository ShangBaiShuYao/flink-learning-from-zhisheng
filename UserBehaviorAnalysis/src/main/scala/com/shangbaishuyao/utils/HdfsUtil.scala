package com.shangbaishuyao.utils

import java.io.{BufferedReader, InputStreamReader}
import java.util
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

object HdfsUtil {
  private val LOGGER: Logger = LoggerFactory.getLogger(HdfsUtil.getClass)

  /**
   * 是否目录
   **/
  def isDir(hdfs: FileSystem, path: String): Boolean = {
    if (StringUtils.isEmpty(path)) {
      return false
    }

    hdfs.isDirectory(new Path(path))
  }

  /**
   * 是否目录
   **/
  def isDir(hdfs: FileSystem, path: Path): Boolean = {
    hdfs.isDirectory(path)
  }

  /**
   * 是否文件
   **/
  def isFile(hdfs: FileSystem, path: String): Boolean = {
    if (StringUtils.isEmpty(path)) {
      return false
    }

    hdfs.isFile(new Path(path))
  }

  /**
   * 是否文件
   **/
  def isFile(hdfs: FileSystem, path: Path): Boolean = {
    hdfs.isFile(path)
  }

  /**
   * 创建文件
   **/
  def createFile(hdfs: FileSystem, path: String): Boolean = {
    if (StringUtils.isEmpty(path)) {
      return false
    }

    hdfs.createNewFile(new Path(path))
  }

  /**
   * 创建文件
   **/
  def createFile(hdfs: FileSystem, path: Path): Boolean = {
    hdfs.createNewFile(path)
  }

  /**
   * 创建目录
   **/
  def createDir(hdfs: FileSystem, path: String): Boolean = {
    if (StringUtils.isEmpty(path)) {
      return false
    }

    hdfs.mkdirs(new Path(path))
  }

  /**
   * 创建目录
   **/
  def createDir(hdfs: FileSystem, path: Path): Boolean = {
    hdfs.mkdirs(path)
  }

  /**
   * 文件是否存在
   **/
  def exists(hdfs: FileSystem, path: String): Boolean = {
    if (StringUtils.isEmpty(path)) {
      return false
    }

    hdfs.exists(new Path(path))
  }

  /**
   * 文件是否存在
   **/
  def exists(hdfs: FileSystem, path: Path): Boolean = {
    hdfs.exists(path)
  }

  /**
   * 删除
   **/
  def delete(hdfs: FileSystem, path: String): Boolean = {
    if (StringUtils.isEmpty(path)) {
      return false
    }

    hdfs.delete(new Path(path), true)
  }

  /**
   * 删除
   **/
  def delete(hdfs: FileSystem, path: Path): Boolean = {
    hdfs.delete(path, true)
  }

  /**
   * 追加写入
   **/
  def append(hdfs: FileSystem, path: String, content: String): Boolean = {
    if (StringUtils.isEmpty(path) || content == null) {
      return false
    }

    if (!exists(hdfs, path)) {
      createFile(hdfs, path)
    }
    hdfs.getConf.setBoolean("dfs.support.append", true)

    try {
      val append = hdfs.append(new Path(path))
      append.write(content.getBytes("UTF-8"))
      append.write(10) // 换行
      append.flush()
      append.close()
      true
    } catch {
      case e: Exception => {
        LOGGER.error(s"append file exception, path{$path}, content{$content}", e)
        false
      }
    }
  }

  /**
   * 读取文件
   **/
  def read(hdfs: FileSystem, file: String): Array[Byte] = {
    val result = new Array[Byte](0)
    if (StringUtils.isEmpty(file)) {
      return result
    }
    if (exists(hdfs, file)) {
      return result
    }

    var isr: InputStreamReader = null
    var br: BufferedReader = null
    try {
      val path = new Path(file)
      val inputStream = hdfs.open(path)
      isr = new InputStreamReader(inputStream)
      br = new BufferedReader(isr)

      var content = new StringBuilder
      var line: String = br.readLine()
      while (line != null) {
        content ++= line
        line = br.readLine()
      }

      br.close()
      isr.close()

      content.toString().getBytes("UTF-8")
    } catch {
      case e: Exception => {
        LOGGER.error(s"read file exception, file{$file}", e)
        result
      }
    } finally {
      try {
        isr.close()
      } catch {
        case _: Exception => {}
      }
      try {
        br.close()
      } catch {
        case _: Exception => {}
      }
    }
  }

  /**
   * 上传本地文件
   **/
  def uploadLocalFile(hdfs: FileSystem, localPath: String, hdfsPath: String): Boolean = {
    if (StringUtils.isEmpty(localPath) || StringUtils.isEmpty(hdfsPath)) {
      return false
    }

    val src = new Path(localPath)
    val dst = new Path(hdfsPath)
    hdfs.copyFromLocalFile(src, dst)
    true
  }

  /**
   * 列出目录下所有文件
   **/
  def list(hdfs: FileSystem, path: String): util.List[String] = {
    val list: util.List[String] = new util.ArrayList[String]()
    if (StringUtils.isEmpty(path)) {
      return list
    }

    val stats = hdfs.listStatus(new Path(path))
    for (i <- 0 to stats.length - 1) {
      if (stats(i).isFile) {
        // path.getName，只是文件名，不包括路径
        // path.getParent，只是父文件的文件名，不包括路径
        // path.toString，完整的路径名
        list.add(stats(i).getPath.toString)
      } else if (stats(i).isDirectory) {
        list.add(stats(i).getPath.toString)
      } else if (stats(i).isSymlink) {
        list.add(stats(i).getPath.toString)
      }
    }
    list
  }
}