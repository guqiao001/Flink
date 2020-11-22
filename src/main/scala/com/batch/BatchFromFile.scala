/*
package com.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
object BatchFromFile {
  def main(args: Array[String]): Unit = {
    //获取env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //1.读取本地文件
    val ds1: DataSet[String] = env.readTextFile("E:\\software\\BigDataCode\\LU76.12_Flink\\src\\main\\scala\\batch\\input\\word.txt")
  //  ds1.print()

    //2.读取HDFS文件
    val ds2: DataSet[String] = env.readTextFile("hdfs://master:8020/wordcount/input/words.txt")
 //   ds2.print()

    //3.读取CSV文件
    case class Student(id:Int, name:String)
    import org.apache.flink.api.scala._
    val ds3: DataSet[Student] = env.readCsvFile[Student]("E:\\software\\BigDataCode\\LU76.12_Flink\\src\\main\\scala\\batch\\input\\subject.csv")
    ds3.print()

    //4.读取压缩文件
    val ds4 = env.readTextFile("E:\\software\\BigDataCode\\LU76.12_Flink\\src\\main\\scala\\batch\\input\\wordcount.txt.gz")
   // ds4.print()

    //5.读取文件夹
    val parameters = new Configuration
    parameters.setBoolean("recursive.file.enumeration", true)
    val ds5 = env.readTextFile("E:\\software\\BigDataCode\\LU76.12_Flink\\src\\main\\scala\\batch\\input\\wc").withParameters(parameters)
   // ds5.print()

  }



}
*/
