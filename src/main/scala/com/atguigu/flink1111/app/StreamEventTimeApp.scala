package com.atguigu.flink1111.app

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object StreamEventTimeApp {

  def main(args: Array[String]): Unit = {
    //  环境
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    // 声明使用 eventTime
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    val dstream: DataStream[String] = env.socketTextStream("hadoop1",7777)

    //  环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
   // env.setParallelism(1)

    val dstream: DataStream[String] = env.socketTextStream("hadoop1",7777)

    import org.apache.flink.api.scala._


    val textWithTsDstream: DataStream[(String, Long, Int)] = dstream.map { text =>
      val arr: Array[String] = text.split(" ")
      (arr(0), arr(1).toLong, 1)
    }

    // 1 告知 flink如何获取数据中的event时间戳  2 告知延迟的watermark
    val textWithEventTimeDstream: DataStream[(String, Long, Int)] = textWithTsDstream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(3000L)) {
      override def extractTimestamp(element: (String, Long, Int)): Long = {

        return  element._2
      }
    }).setParallelism(1)

    //每5秒开一个窗口 统计key的个数  5秒是一个数据的时间戳为准
    val textKeyStream: KeyedStream[(String, Long, Int), Tuple] = textWithEventTimeDstream.keyBy(0)
    textKeyStream.print("textkey:")
   //滚动窗口
   // val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textKeyStream.window( TumblingEventTimeWindows.of(Time.milliseconds(5000L))  )
   //滑动窗口
    //val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textKeyStream.window( SlidingEventTimeWindows.of(Time.milliseconds(5000L),Time.milliseconds(1000L))  )
   //会话窗口
   val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textKeyStream.window( EventTimeSessionWindows.withGap(Time.milliseconds(5000L))   )

    windowStream.sum(2) .print("windows:::").setParallelism(1)


    env.execute()

  }

}
