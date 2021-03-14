package com.atguigu.gmall1021.realtime.util

import java.util
import java.util.Properties

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder

object MyEsUtil {

  var jestClientFactory: JestClientFactory = null

  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val esServer: String = properties.getProperty("es.server")

  //获得从工厂中获得连接
  def getJestClient(): JestClient = {
    if (jestClientFactory == null) {
      //创建工厂
      jestClientFactory = new JestClientFactory
      jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder(esServer) //多个节点可以放在一个list或者set集合中
        .multiThreaded(true).maxTotalConnection(10).build())
    }
    jestClientFactory.getObject
  }

  //单条保存
  def save(): Unit = {
    val jestClient: JestClient = getJestClient()
    //创建一个插入动作 index
    //Builder中Any 要放可以被转化为json的类型（Map，JSONobject,case class）
    val index: Index = new Index.Builder(Movie("0104", "哪吒重生")).index("movie_test1021_20210304").`type`("_doc").build()
    jestClient.execute(index)
    jestClient.close()
  }

  //整批保存 batch = bulk
  //幂等性 保存一个和保存N次是一样的 -->识别 相同的数据-->靠在索引范围内唯一性的字段
  def saveBulk(dataList: List[(String, Any)], indexName: String): Unit = {
    if (dataList != null && dataList.size > 0) {
      val jestClient: JestClient = getJestClient()
      val bulkBuilder = new Bulk.Builder()
      //一个批次由多个单次动作组成
      for ((id, data) <- dataList) {
        //把每条数据创建为单个的插入动作  //关于幂等性 加入id ,相同id的数据，后面会覆盖之前的数据，不会产生重复
        val index: Index = new Index.Builder(data).index(indexName).`type`("_doc").id(id).build()
        bulkBuilder.addAction(index) //插入动作加入批次中
      }
      val bulk: Bulk = bulkBuilder.build() //封装
      val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulk).getItems
      println("es保存" + items.size() + "条数据")
      jestClient.close()
    }
  }

  def search(): Unit = {
    val jestClient: JestClient = getJestClient()
    //创建一个查询动作 search
    // 参数 ： 1 查询条件 2 索引名
    //推荐使用工具封装查询条件
    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.query(new MatchQueryBuilder("movie_name", "哪吒"))

    // val query="{\n  \"query\": {\n    \"match\": {\n      \"movie_name\": \"哪吒\"\n    }\n  }\n}"
    val search: Search = new Search.Builder(searchSourceBuilder.toString()).addIndex("movie_test1021_20210304").build()
    val result: SearchResult = jestClient.execute(search)
    // 解析返回结果
    val list: util.List[SearchResult#Hit[Movie, Void]] = result.getHits(classOf[Movie])
    import collection.JavaConverters._
    for (hit <- list.asScala) {
      val movie: Movie = hit.source
      println(movie)
    }
    jestClient.close()
  }

  def main(args: Array[String]): Unit = {
    search()
  }

  case class Movie(id: String, movie_name: String)


}
