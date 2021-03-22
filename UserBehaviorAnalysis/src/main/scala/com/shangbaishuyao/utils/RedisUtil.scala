package com.shangbaishuyao.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Desc: 连接redis工具类<br/>
 * create by shangbaishuyao on 2021/3/14
 * @Author: 上白书妖
 * @Date: 18:05 2021/3/14
 */
object RedisUtil {
  var jedisPool: JedisPool = _
  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      println("================开辟一个连接池==================")
      val config: Properties = PropertiesUtil.load("config.properties")
      val host: String = config.getProperty("redis.host")
      val port: String = config.getProperty("redis.port")
      val password: String = config.getProperty("redis.password")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt,600,password)
    }
    //    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    //    println("获得一个连接")
    jedisPool.getResource
  }
}

