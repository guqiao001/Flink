package com.stream.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object ValueStateWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    /**
      * 使用MapState保存中间结果对下面数据进行分组求和
      * 1.获取流处理执行环境
      * 2.加载数据源
      * 3.数据分组
      * 4.数据转换，定义MapState,保存中间结果
      * 5.数据打印
      * 6.触发执行
      */
    val source: DataStream[(String, Int)] = env.fromCollection(List(
      ("java", 1),
      ("python", 3),
      ("java", 2),
      ("scala", 2),
      ("python", 1),
      ("java", 1),
      ("scala", 2)))

    source.keyBy(0)
      .map(new RichMapFunction[(String, Int), (String, Int)] {
        var mste: ValueState[Int] = _

        override def open(parameters: Configuration): Unit = {
          val msState = new ValueStateDescriptor[Int]("ms", TypeInformation.of(new TypeHint[(Int)] {}))
          mste=getRuntimeContext.getState(msState)
        }

        override def map(value: (String, Int)): (String, Int) = {
          val i: Int = mste.value()
          if (i != null) {
            val count = i + value._2
            mste.update(count)
            return (value._1, count)
          }
          mste.update(value._2)
          value
        }
      }).print()

    env.execute()
  }

}
