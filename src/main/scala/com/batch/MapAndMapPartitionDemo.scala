package com.batch

import org.apache.flink.api.scala.ExecutionEnvironment

object MapAndMapPartitionDemo {
  def main(args: Array[String]): Unit = {
    //获取env
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //map
    val data: DataSet[String] = env.fromCollection(List("1,张三", "2,李四", "3,王五", "4,赵六"))
    case class User(id: String, name: String)
    val userDataSet: DataSet[User] = data.map(text => {
      val files = text.split(",")
      User(files(0), files(1))
    })
    userDataSet.print()

    //mapPartition
    val userDataSet2 = data.mapPartition(iter => {
      // TODO:打开连接
      iter.map(ele => {
        val files = ele.split(",")
        User(files(0), files(1))
      })
      // TODO：关闭连接
    }
    )
    userDataSet2.print()

  }



}
