package com.stream.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
/*
* 需求
每5秒钟统计一次，在这过去的5秒钟内，各个路口通过红绿灯汽车的数量--滚动窗口
每5秒钟统计一次，在这过去的10秒钟内，各个路口通过红绿灯汽车的数量--滑动窗口
会话窗口(需要事件时间支持):在30秒内无数据接入则触发窗口计算
* */
object StreamingTimeWindow {
  //样例类CarWC(信号灯id,数量)
  case class CarWc(sensorId: Int, carCnt: Int)

  def main(args: Array[String]): Unit = {
    //1.准备环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2.接收数据
    val socketData = env.socketTextStream("localhost", 9999)
    //3.处理数据socketData->carWcData
    import org.apache.flink.api.scala._
    val carData: DataStream[CarWc] = socketData.map(line => {
      val arr: Array[String] = line.split(",")
      CarWc(arr(0).toInt, arr(1).toInt)
    })
    //nokeyed数据
    //val value: AllWindowedStream[CarWc, TimeWindow] = carData.timeWindowAll(Time.seconds(5),Time.seconds(5))
    //keyed数据
    //val value1: WindowedStream[CarWc, Tuple, TimeWindow] = carData.keyBy(0).timeWindow(Time.seconds(5),Time.seconds(5))

    //4.窗口聚合
    //4.1 滚动窗口
    //每5秒钟统计一次，在这过去的5秒钟内，各个路口通过红绿灯汽车的数量
    //无重叠数据，所以只需要给一个参数即可，每5秒钟统计一下各个路口通过红绿灯汽车的数量
    val result1: DataStream[CarWc] = carData.keyBy(0)
      //.timeWindow(Time.seconds(5),Time.seconds(5))
      .timeWindow(Time.seconds(5))
      .sum(1)
    result1.print()

    //4.2 滑动窗口
    //每5秒钟统计一次，在这过去的10秒钟内，各个路口通过红绿灯汽车的数量。
    /*val result2: DataStream[CarWc] = carData.keyBy(0)
      .timeWindow(Time.seconds(10),Time.seconds(5))
      .sum(1)
    result2.print()*/

    //4.3 会话窗口(需要时间事件支持)
    //指定会话超时，即会话之间的时间间隔，是指在规定的时间内如果没有数据活跃接入，则认为窗口结束，触发窗口计算
    // .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
    //如果有时间事件,滚动窗口和滑动窗口也可以使用如下API
    //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))

    //5.启动执行
    env.execute()
  }

}
