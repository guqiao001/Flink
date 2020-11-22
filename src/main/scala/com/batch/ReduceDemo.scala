package com.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.streaming.api.scala._
object ReduceDemo {
  def main(args: Array[String]): Unit = {
    //reduce
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val wordCountDataSet: DataSet[(String, Int)] = env.fromCollection(List(("java", 1) , ("java", 1) ,("java", 1)))
    val resultDataSet3 = wordCountDataSet.reduce((wc1, wc2) =>(wc2._1, wc1._2 + wc2._2))
   // resultDataSet3.print()

    //groupBy+reduce
    val wordcountDataSet2: DataSet[(String, Int)] = env.fromCollection(List(("java", 1) , ("java", 1) ,("scala", 1)))
    val groupedDataSet: GroupedDataSet[(String, Int)] = wordcountDataSet2.groupBy(_._1)
    //val resultDataSet4: DataSet[(String, Int)] = groupedDataSet.reduce((t1, t2) =>(t1._1, t1._2 + t2._2))
    //resultDataSet4.print()

    //groupBy+sum
    val wordcountDataSet3: DataSet[(String, Int)] = env.fromCollection(List(("java", 1) , ("java", 1) ,("scala", 1)))
    val resultDataSet5: DataSet[(String, Int)]= wordcountDataSet3.groupBy(0).sum(1)
    resultDataSet5.print()

  }
}
