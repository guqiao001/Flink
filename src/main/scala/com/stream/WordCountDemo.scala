package com.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    //创建一个流处理的运行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //构建socket source数据源，返回值类型datastream
    val socketDs: DataStream[String] = environment.socketTextStream("localhost", 8888)
    //接收到的数据转为（单词，1）
    val tupleDs: DataStream[(String, Int)] = socketDs.flatMap(_.split(" ")).map((_, 1))
    //对元组使用keyby分组（类似批处理中的groupby）
    val keyedStrem: KeyedStream[(String, Int), Tuple] = tupleDs.keyBy(0)
    //，使用窗口进行5s的计算每5秒计算一次
    val windowStrem: WindowedStream[(String, Int), Tuple, TimeWindow] = keyedStrem.timeWindow(Time.seconds(5))
    //sum单词数量
    val resDs: DataStream[(String, Int)] = windowStrem.sum(1)
    //打印
    resDs.print()
    //执行
    environment.execute()
  }
}
