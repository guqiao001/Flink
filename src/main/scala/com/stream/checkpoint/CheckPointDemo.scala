package com.stream.checkpoint

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //开启ck,指定使用hdfsstatebackend
    streamEnv.setStateBackend(new FsStateBackend("hdfs://master:9000/checkpoint"))
//设置ck的周期间隔，默认是没有开启ck,需要指定间隔来开启ck
    streamEnv.enableCheckpointing(1000)
    //设置ck的执行语义，最多一次，至少一次，精确一次
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置两次ck之间的最小时间间隔，两次ck之间时间最少差500ms
    streamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    //设置ck的超时时间，如果超时则认为本次ck失败，继续下一次ck即可，超时60s
    streamEnv.getCheckpointConfig.setCheckpointTimeout(60000)
    //设置ck出现问题，是否让程序报错还是继续任务进行下一次 ck,true:让程序报错，false：不报错进行下次ck
    //如果是false就是ck出现问题我们允许程序继续执行，如果下次ck成功则没有问题，但是如果程序下次ck也没有成功，
    //此时程序挂掉需要从ck中恢复数据时可能导致程序计算错误，或者是重复计算
    streamEnv.getCheckpointConfig.setFailOnCheckpointingErrors(false)

















  }
}
