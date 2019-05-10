package com.atguigu.flink1111.app

import com.alibaba.fastjson.JSON
import com.atguigu.flink1111.bean.StartupLog
import com.atguigu.flink1111.util.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
object TableApp {


  def main(args: Array[String]): Unit = {
    //sparkcontext
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //时间特性改为eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val myKafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")
    val dstream: DataStream[String] = env.addSource(myKafkaConsumer)

    val startupLogDstream: DataStream[StartupLog] = dstream.map{ jsonString =>JSON.parseObject(jsonString,classOf[StartupLog]) }
    //告知watermark 和 eventTime如何提取
    val startupLogWithEventTimeDStream: DataStream[StartupLog] = startupLogDstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StartupLog](Time.seconds(0L)) {
      override def extractTimestamp(element: StartupLog): Long = {
        element.ts
      }
    }).setParallelism(1)

    //SparkSession
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //把数据流转化成Table
    val startupTable: Table = tableEnv.fromDataStream(startupLogWithEventTimeDStream , 'mid,'uid,'appid,'area,'os,'ch,'logType,'vs,'logDate,'logHour,'logHourMinute,'ts.rowtime)

    //通过table api 进行操作
    // 每10秒 统计一次各个渠道的个数 table api 解决
    //1 groupby  2 要用 window   3 用eventtime来确定开窗时间
    val resultTable: Table = startupTable.window(Tumble over 10000.millis on 'ts as 'tt).groupBy('ch,'tt ).select( 'ch, 'ch.count)
   // 通过sql 进行操作

    val resultSQLTable : Table = tableEnv.sqlQuery( "select ch ,count(ch)   from "+startupTable+"  group by ch   ,Tumble(ts,interval '10' SECOND )")

    //把Table转化成数据流
    //val appstoreDStream: DataStream[(String, String, Long)] = appstoreTable.toAppendStream[(String,String,Long)]
    val resultDstream: DataStream[(Boolean, (String, Long))] = resultSQLTable.toRetractStream[(String,Long)]

    resultDstream.filter(_._1).print()

    env.execute()

  }
}
