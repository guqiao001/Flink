/*
package com.stream.watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
case class CarWcSample(id: String, num: Int, ts: Long)

object SlideOutputDemo1 {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //设置处理时间为事件时间
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //生成水印的周期，默认200ms
    streamEnv.getConfig.setAutoWatermarkInterval(200)
    //默认程序并行度是机器的核数，8个并行度，注意在flink程序中如果是多并行度，水印时间是每个并行度比较最小的值作为当前流的watermark
    streamEnv.setParallelism(1)
    val socketDs: DataStream[String] = streamEnv.socketTextStream("localhost", 8888)
    //数据处理之后添加水印
    val CarWcSampleDs: DataStream[CarWcSample] = socketDs.map(line => {
      val arr: Array[String] = line.split(" ")
      CarWcSample(arr(0), arr(1).trim.toInt, arr(2).trim.toLong)
    })
    //添加水印 周期性 AssignerWithPeriodicWatermarks 使用其子类，构造参数：水印允许的延迟时间,泛型是steam中的数据类型
    val waterMarkDs: DataStream[CarWcSample] = CarWcSampleDs.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[CarWcSample] {
        //watermark=eventtime-延迟时间
        //定义允许延迟的时间2s
        val delayTime = 2000
        //定义当前最大的时间戳
        var currentMaxTimeStamp = 0L
        private var lastEmittedWatermark = Long.MinValue

        //获取watermark时间 实现watermark不会倒退
        override def getCurrentWatermark: Watermark = {
          //计算watermark
          val waterMarkTime = currentMaxTimeStamp - delayTime
          if (waterMarkTime > lastEmittedWatermark) {
            lastEmittedWatermark = waterMarkTime
          }
          new Watermark(lastEmittedWatermark)
        }

        //抽取时间戳 计算watermark
        //element新到达的元素 previousElementTimestamp之前元素的时间戳
        override def extractTimestamp(element: CarWcSample, previousElementTimestamp: Long): Long = {
          //获取时间
          //注意的问题：时间倒退的问题：消息过来是乱序的，每次新来的消息时间戳不是一定变大的，所以会导致水印有可能倒退
          var eventTime = element.ts
          //比较与之前最大的时间戳进行比较
          if (eventTime > currentMaxTimeStamp) {
            currentMaxTimeStamp = eventTime
          }
          eventTime
        }
      }


    )
  //在水印基础之上，使用窗口触发之后，在继续等待5s,然后设置侧道输出，如果5s之后数据才到就进入侧道输出，不会再触发窗口计算
    //设置窗口 5秒的滚动窗口
    val outputTag = new OutputTag[CarWcSample]("lateCarWcSample")
    val windowStream: WindowedStream[CarWcSample, Tuple, TimeWindow] = waterMarkDs.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //设置允许延迟时间 在水印基础上再次增加延迟时间允许延迟时间
      .allowedLateness(Time.seconds(5))
      //设置侧输出
      .sideOutputLateData(outputTag)
    //使用apply方法对窗口进行计算
    val windowDs: DataStream[CarWcSample] = windowStream.apply(
      //泛型：1.CarWcSample，2.CarWcSample,3.Tuple,4.TimeWindow
      new WindowFunction[CarWcSample, CarWcSample, Tuple, TimeWindow] {
        //key:tuple，window:当前触发计算的window对象，input:当前窗口的数据，out:计算结果收集器
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[CarWcSample], out: Collector[CarWcSample]): Unit = {
          val wc: CarWcSample = input.reduce((c1, c2) => {
            CarWcSample(c1.id, c1.num + c2.num, c2.ts) //累加出通过的汽车数量，关于时间在这我们不关心
          })
          out.collect(wc)
          //获取到窗口开始和结束时间
          println("窗口开始时间:" + window.getStart + ",窗口结束时间:" + window.getEnd + ",窗口中的数据:" + input.iterator.mkString("@"))
        }
      })
    windowDs.print()
    //获取侧道输出数据
    val lateCarWcSample: DataStream[CarWcSample] = windowDs.getSideOutput(outputTag)
    //打印侧道输出数据
    lateCarWcSample.printToErr("侧道输出：")
    streamEnv.execute()

  }
}
*/
