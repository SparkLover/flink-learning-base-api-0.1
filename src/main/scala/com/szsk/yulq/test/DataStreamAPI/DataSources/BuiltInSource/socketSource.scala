package com.szsk.yulq.test.DataStreamAPI.DataSources.BuiltInSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object socketSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    从socket中获取数据
    env.socketTextStream("localhost",8888)

  }


}
