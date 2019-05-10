package com.atguigu.flink1111.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.flink1111.bean.StartupLog
import com.atguigu.flink1111.util.{MyEsUtil, MyKafkaUtil, MyRedisUtil, MyjdbcSink}
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink


object StartupApp {

  def main(args: Array[String]): Unit = {

      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

      val myKafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")
      val dstream: DataStream[String] = env.addSource(myKafkaConsumer)

    val startupLogDstream: DataStream[StartupLog] = dstream.map{ jsonString =>JSON.parseObject(jsonString,classOf[StartupLog]) }

    //需求一 相同渠道的值进行累加
//    val chCountDstream: DataStream[(String, Int)] = startupLogDstream.map { startuplog => (startuplog.ch, 1) }.keyBy(0)
//      .reduce { (startuplogCount1, startuplogCount2) =>
//        val newCount: Int = startuplogCount1._2 + startuplogCount2._2
//        (startuplogCount1._1, newCount)
//    }

     // val sumDstream: DataStream[(String, Int)] = startupLogDstream.map { startuplog => (startuplog.ch, 1) }.keyBy(0).sum(1)
    //需求二   把 appstore   和其他的渠道的数据 分成两个流
    val splitStream: SplitStream[StartupLog] = startupLogDstream.split { startupLog =>
      var flagList: List[String] = List()
      if (startupLog.ch.equals("appstore")) {
        flagList = List("apple")
      } else {
        flagList = List("other")
      }
      flagList
    }
    val appleStream: DataStream[StartupLog] = splitStream.select("apple")
   // appleStream.print("this is apple: ").setParallelism(1)
    val otherStream: DataStream[StartupLog] = splitStream.select("other")
  //  otherStream.print("this is other ").setParallelism(1)


//    val connStream: ConnectedStreams[StartupLog, StartupLog] = appleStream.connect(otherStream)
//    val allDataStream: DataStream[String] = connStream.map(
//      (startuplog1: StartupLog) => startuplog1.ch,
//      (startuplog2: StartupLog) => startuplog2.ch
//    )
//    allDataStream.print("all:")

    val unionDstream: DataStream[StartupLog] = appleStream.union(otherStream)
   // unionDstream.print("union:")

   // sink之一 用sink保存到kafka
    unionDstream.map(_.toString).addSink(MyKafkaUtil.getProducer("gmall_union"))


   // sumDstream.print().setParallelism(1)

    //把按渠道的统计值保存到redis中  hash   key: channel_sum  field ch  value: count
    val chCountDstream: DataStream[(String, Int)] = startupLogDstream.map { startuplog => (startuplog.ch, 1) }.keyBy(0).sum(1)
    val channelSumDataStream: DataStream[(String, String)] = chCountDstream.map(chCount=>(chCount._1,chCount._2.toString()))

  // sink之二 用sink 保存到redis
   channelSumDataStream.addSink( MyRedisUtil.getRedisSink())
 //  dstream.print().setParallelism(1)

    //sink之三  保存到ES
    val esSink: ElasticsearchSink[String] = MyEsUtil.getEsSink("gmall1111_startup")
    //dstream.addSink(esSink)

    //sink之四 保存到Mysql中
    //startupLogDstream.map(startuplog=>Array(startuplog.mid,startuplog.uid,startuplog.ch ,startuplog.area,startuplog.ts))
    //  .addSink(new MyjdbcSink("insert into z_startup values(?,?,?,?,?)"))


    //需求: 相同渠道的值进行累加 每10秒进行一次累加
//         val chKeyedStream: KeyedStream[(String, Int), Tuple] = startupLogDstream.map { startuplog => (startuplog.ch, 1) }.keyBy(0)
//
//         val window10Stream: WindowedStream[(String, Int), Tuple, TimeWindow] = chKeyedStream.timeWindow(Time.seconds(10L),Time.seconds(3L))
//
//         val channalSumDtream: DataStream[(String, Int)] = window10Stream.sum(1)
//
//         channalSumDtream.print("10秒的channal_sum:")

    //需求： 每个key的个数达到 count数值的时候 进行一次开窗
    val chKeyedStream: KeyedStream[(String, Int), Tuple] = startupLogDstream.map { startuplog => (startuplog.ch, 1) }.keyBy(0)

     val chCountKeyWinodow: WindowedStream[(String, Int), Tuple, GlobalWindow] = chKeyedStream.countWindow(10L,2L)

    val channalSumDtream: DataStream[(String, Int)] = chCountKeyWinodow.sum(1)

    channalSumDtream.print("channal_sum:")


         env.execute()

  }

}
