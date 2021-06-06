package com.shangbaishuyao.Handler

import java.io.File
import com.alibaba.fastjson.JSON
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner

class LiuLiangUserDetailBucketAssigner extends BucketAssigner[String,String] {
  override def getBucketId(in: String, context: BucketAssigner.Context): String = {
    System.out.println(in)
    in
  }

  override def getSerializer: SimpleVersionedSerializer[String] = new LiuLiangStringSerializer
  def main(args: Array[String]): Unit = {}
}