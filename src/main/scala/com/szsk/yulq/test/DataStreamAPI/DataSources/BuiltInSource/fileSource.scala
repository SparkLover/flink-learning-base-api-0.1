package com.szsk.yulq.test.DataStreamAPI.DataSources.BuiltInSource

import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType

object fileSource {
  def main(args: Array[String]): Unit = {
    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //    直接读取文本文件
    val textStream: DataStreamSource[String] = env.readTextFile("/user/local/data_example.log")

    //通过指定CSVInputFormat读取csv文件
    env.readFile(new CsvInputFormat[String](
      new Path("/user/local/data_example.log")
    ) {
      override def fillRecord(out: String, objects: Array[AnyRef]): String =  {
        return null
      }
    },"/user/local/data_example.log")
  }
}
