package com.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object StreamWordCout {
  def main(args: Array[String]): Unit = {
    //1.初始化流计算的环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //执行环境层面设置并行度
    streamEnv.setParallelism(1) //默认所有算子的并行度为1

    //2.导入隐式转换
    import org.apache.flink.streaming.api.scala._
    //3.读取数据
    //DataStream等价于spark中的Dstream
    val stream: DataStream[String] = streamEnv.socketTextStream("localhost", 8888)
    //4.转换和处理数据
    val result: DataStream[(String, Int)] = stream.flatMap(_.split(" "))
      .map((_, 1)).setParallelism(1) //算在层面设置并行度
      .keyBy(0)
     .sum(1).setParallelism(1)
    //5.打印结果
    result.print().setParallelism(1)
   // result.print()

    //6.启动流计算程序
    streamEnv.execute("wordcount")

  }
}
