package com.shangbaishuyao.utils

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

import scala.collection.JavaConversions._

/**
 * Desc: 连接ElasticSearch工具类 <br/>
 * 在es里面提前建立好索引
 * create by shangbaishuyao on 2021/3/17
 * @Author: 上白书妖
 * @Date: 12:00 2021/3/17
 */
object MyElasticSearchUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _
  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }
  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(
      new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(200) //连接总数
      .connTimeout(10000)
      .readTimeout(10000).build)
  }

  // 批量插入数据到ES
  /***
   * @param indexName
   * @param docList : List[(String, Any)] 文档list("_doc"),list里面的string类型是id, Any是es里面的source
   */
  def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {
    if (docList.nonEmpty) {
      //获取客户端
      val jest: JestClient = getClient
      //创建BulkBuild对象
      val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
      //循环添加Index
      for ((id, doc) <- docList) {
        val indexBuilder = new Index.Builder(doc)
        if (id != null) {
          indexBuilder.id(id)
        }
        val index: Index = indexBuilder.build()
        bulkBuilder.addAction(index)
      }
      //创建Bulk对象
      val bulk: Bulk = bulkBuilder.build()
      var items: util.List[BulkResult#BulkResultItem] = null
      try {
        items = jest.execute(bulkBuilder.build()).getItems
      } catch {
        case ex: Exception => println(ex.toString)
      } finally {
        close(jest)
        println("保存" + items.size() + "条数据")
        for (item <- items) {
          if (item.error != null && item.error.nonEmpty) {
            println(item.error)
            println(item.errorReason)
          }
        }
      }
    }
  }
}
