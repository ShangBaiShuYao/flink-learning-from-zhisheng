package com.shangbaishuyao.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * Desc: 读取配置信息 <br/>
 * create by shangbaishuyao on 2021/3/14
 * @Author: 上白书妖
 * @Date: 18:05 2021/3/14
 */
object PropertiesUtil {
  def load(propertieName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
    prop
  }
}

