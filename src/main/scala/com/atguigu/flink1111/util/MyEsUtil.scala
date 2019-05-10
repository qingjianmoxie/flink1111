package com.atguigu.flink1111.util

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object MyEsUtil {

  val hostList:util.List[HttpHost] = new util.ArrayList[HttpHost]()
  hostList.add(new HttpHost("hadoop1",9200,"http"))
  hostList.add(new HttpHost("hadoop2",9200,"http"))
  hostList.add(new HttpHost("hadoop3",9200,"http"))

  def getEsSink(indexName:String): ElasticsearchSink[String] ={


    val esSinkFunc = new ElasticsearchSinkFunction[String] {
      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        val jSONObject: JSONObject = JSON.parseObject(element)
        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(jSONObject)
        indexer.add(indexRequest)
      }
    }

       val esSinkBuilder = new ElasticsearchSink.Builder[String](hostList,esSinkFunc)

       esSinkBuilder.setBulkFlushMaxActions(10)

       val esSink: ElasticsearchSink[String] = esSinkBuilder.build()

       esSink
  }

}
