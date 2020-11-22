package org.doit

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._

object MapWithState {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.enableCheckpointing(5000)
    streamEnv.setStateBackend(new FsStateBackend("file:///E:\\software\\BigDataCode\\LU76.12_FlinkClient\\src\\main\\scala\\org\\doit\\checkpoint\\backend"))
    val lines: DataStream[String] = streamEnv.socketTextStream("192.168.100.251", 8888)
    val keyed: KeyedStream[Tuple2[String, Int], Tuple] = lines.map(line => {
      if ("stotm".startsWith(line)) {
        println(1 / 0)
      }
      (line, 1)
    }).keyBy(0)
    val summed: DataStream[(String, Int)] = keyed.mapWithState((input: (String, Int), state: Option[Int]) => {
      state match {
        case Some(count) => {
          val key = input._1
          val value = input._2
          val sum = count + value
          ((key, sum), Some(sum))
        }
        case None => {
          (input, Some(input._2))
        }
      }
    })
    summed.print()
    streamEnv.execute()

  }


}
