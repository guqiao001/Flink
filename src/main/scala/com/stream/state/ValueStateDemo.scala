package com.stream.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object ValueStateDemo {
  //使用keyedstate来计算出数据流中的最大值
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val senv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    senv.setParallelism(1)
    //2.接收数据
    val socketDs: DataStream[String] = senv.socketTextStream("node1", 9999)
    //3.处理数据
    val keyedStream: KeyedStream[(String, Int), Tuple] = socketDs.map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0), arr(1).toInt)
    }).keyBy(0)


    //4.获取最大值
    //    val resDs: DataStream[(String, Int)] = keyedStream.max(1)
    //自定义实现获取最大值
    val resDs: DataStream[(String, Int)] = keyedStream.map(
      new RichMapFunction[(String, Int), (String, Int)] {

        //4.1 声明一个state结构用来存储之前的最大值,无需关注key是什么，只需要关心value值
        private var maxValueState: ValueState[Int] = null

        //4.2 open方法中获取状态
        override def open(parameters: Configuration): Unit = {
          //4.2.1 准备一个valuestatedescriptor
          //构造 def this(name: String, typeClass: Class[T]) { this()     super (name, typeClass, null)

          val maxValueDesc = new ValueStateDescriptor[Int]("maxValues", classOf[Int])
          //4.2.2 根据描述器获取到valuestate
          maxValueState = getRuntimeContext.getState(maxValueDesc)
        }

        //4.2 把新数据与state中的老数据进行比较
        override def map(input: (String, Int)): (String, Int) = {

          //获取到state中的值
          val oldValue: Int = maxValueState.value()
          if (input._2 > oldValue) maxValueState.update(input._2)
          (input._1, maxValueState.value())
        }
      }
    )
    resDs.print("max结果》》")
    //5.触发执行
    senv.execute()


  }


}
