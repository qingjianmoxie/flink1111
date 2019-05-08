package com.atguigu.flink1111.app

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWcApp {

  def main(args: Array[String]): Unit = {
    //  环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dstream: DataStream[String] = env.socketTextStream("hadoop1",7777)

    import org.apache.flink.api.scala._
    val dStream: DataStream[(String, Int)] = dstream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    dStream.print().setParallelism(1)

    env.execute()

  }

}
