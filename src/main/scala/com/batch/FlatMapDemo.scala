package com.batch

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
object FlatMapDemo {

  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data2 = environment.fromCollection(List(
      "张三,中国,江西省,南昌市",
      "李四,中国,河北省,石家庄市"
    ))

    //使用flatMap将一条数据转换为三条数据
 /*   val resultDataSet: DataSet[(String, String)] = data2.flatMap(text => {
      val fieldArr = text.split(",")
      List(
        (fieldArr(0), fieldArr(1)),
        (fieldArr(0), fieldArr(1) , fieldArr(2)),
        (fieldArr(0), fieldArr(1) , fieldArr(2) , fieldArr(3))
      )
    }
    )
    resultDataSet.print()*/














  }
}