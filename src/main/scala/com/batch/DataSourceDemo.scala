package com.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object DataSourceDemo {
  /*
  * dataset api中datasource主要有两类
  * 1.基于集合
  * 2.基于文件
  * */
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val eleDs: DataSet[String] = environment.fromElements("hadoop", "saprk", "hive")
    val collDs: DataSet[String] = environment.fromCollection(Array("hadoop", "saprk", "hive"))
    val seqDs: DataSet[Long] = environment.generateSequence(1, 5)
    eleDs.print()
    collDs.print()
    seqDs.print()
    //在批处理中，若sink操作是'count()', 'collect()', or 'print()'，则最后不需要执行execute操作，否则报错
    //因为它们中已经调用了execute()
   // environment.execute()


  }
}
