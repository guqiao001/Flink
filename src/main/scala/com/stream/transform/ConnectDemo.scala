package com.stream.transform

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/*
* 创建两个流，一个产生数值，一个产生字符串，将两个流连接到一起
* connect意义在哪里呢？只是把两个合并为一个，但是处理业务逻辑都是按照自己的方法处理，connect之后两条流可以共享状态数据
*流处理中connect的两个流数据类型可以不同，批处理中union必须要求数据类型一致才能union
*
*
* */
object ConnectDemo {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val numDs: DataStream[Long] = streamEnv.addSource(new MyNumberSource)
    val strDs: DataStream[String] = streamEnv.addSource(new MyStrSource)
    val connectedDs: ConnectedStreams[Long, String] = numDs.connect(strDs)
    //传递两个函数分别处理数据
    val resDs: DataStream[String] = connectedDs.map(l => "long" + l, s => "string" + s)
    resDs.print()
    streamEnv.execute()
  }
}

class MyNumberSource extends SourceFunction[Long] {
  var flag = true
  var num = 1L

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (flag) {
      num += 1
      ctx.collect(num)
      TimeUnit.SECONDS.sleep(1)
    }

  }

  override def cancel(): Unit = {
    flag = false
  }
}

class MyStrSource extends SourceFunction[String] {
  var flag = true
  var num = 1L

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (flag) {
      num += 1
      ctx.collect("str" + num)
      TimeUnit.SECONDS.sleep(1)
    }

  }

  override def cancel(): Unit = {
    flag = false
  }
}