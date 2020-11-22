package com.batch

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, GroupedDataSet}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.api.scala._
object WordCountDemo {
  /*
* 1.獲得一个execution enviroment
* 2.加载/创建初始数据
* 3.指定这些数据的转换
* 4.指定将计算结果放在哪里
* 5.触发程序执行
 */
  def main(args: Array[String]): Unit = {

    //1.获得一个execution enviroment，批处理程序入口对象
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
     environment.setParallelism(1) //若不设置，生成的结果文件数等于当前cpu核数
    //2.加载/创建初始数据
    val sourceDs: DataSet[String] = environment.fromElements("hadoop,flink,hive,flink," +
      "hbase,hive,flume")
    //3.指定这些数据的转换
    val wordsDs: DataSet[String] = sourceDs.flatMap(_.split(","))
    val wordAndOneDs: DataSet[(String, Int)] = wordsDs.map(_ -> 1)
    val groupDs: GroupedDataSet[(String, Int)] = wordAndOneDs.groupBy(0)
    val aggDs: AggregateDataSet[(String, Int)] = groupDs.sum(1)
    //4.指定将计算结果放在哪里
   aggDs.writeAsCsv("E:\\software\\BigDataCode\\LU76.12_Flink\\src\\main\\scala\\batch\\output\\result.txt", writeMode = FileSystem.WriteMode.OVERWRITE)
    //5.触发执行
   environment.execute()
  }
}
