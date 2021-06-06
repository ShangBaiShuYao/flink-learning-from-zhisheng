package com.shangbaishuyao.Handler
import org.apache.flink.core.io.SimpleVersionedSerializer

class LiuLiangStringSerializer extends SimpleVersionedSerializer[String]{
  override def getVersion: Int = 0

  override def serialize(e: String): Array[Byte] = e.getBytes()

  override def deserialize(i: Int, bytes: Array[Byte]): String = {
    if (i != 77){
      throw new Exception("version mismatch")
    }else{
      new String(bytes)
    }
  }

}