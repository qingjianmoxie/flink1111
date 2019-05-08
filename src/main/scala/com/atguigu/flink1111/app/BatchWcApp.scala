package com.atguigu.flink1111.app

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

object BatchWcApp {


  def main(args: Array[String]): Unit = {

    val tool: ParameterTool = ParameterTool.fromArgs(args)

    val inputPath: String = tool.get("input")
    val outputPath:String = tool.get("output")

    //  环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
   //数据源
    val ds: DataSet[String] = env.readTextFile(inputPath)

    import org.apache.flink.api.scala._
    //转换
    val aggsDs: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    // 输出
 //   aggsDs.print()
    aggsDs.writeAsCsv(outputPath).setParallelism(1)

    env.execute()

  }

}
