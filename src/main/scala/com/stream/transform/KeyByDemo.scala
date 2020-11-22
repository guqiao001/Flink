package com.stream.transform

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/*
* keyby类似批处理中的groupby算子，对数据流按照指定规则进行分区
* */
object KeyByDemo {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDs: DataStream[String] = streamEnv.socketTextStream("localhost", 9999)
    val wordAndOneDs: DataStream[(String, Int)] = socketDs.flatMap(_.split(" ")).map((_, 1))
    wordAndOneDs.keyBy(0).sum(1).print()
  //  wordAndOneDs.keyBy(_._1).sum(1).print()
    streamEnv.execute()

  }
}
