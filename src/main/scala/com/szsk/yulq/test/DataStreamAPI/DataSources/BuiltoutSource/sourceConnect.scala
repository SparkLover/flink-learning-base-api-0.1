package com.szsk.yulq.test.DataStreamAPI.DataSources.BuiltoutSource

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08

/**
  * 外部连接器
  * */
object sourceConnect {
  def main(args: Array[String]): Unit = {

//    以kafka为例
    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties: Properties = new Properties()
    /**
      * kafka 的主要连接参数 kafka topic ,bootstrap.servers,zookeeper.connect
      * */
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("zookeeper.connect","localhost:2181")
    properties.setProperty("group.id","test")
    /**
      *
      * Schema 参数的主要作用是根据事先定义好的schema信息将数据序列化成该schema定义的数据类型(String)
      *       默认是 SimpleStringSchema :代表从Kafka中接入的数据转换成String 字符串类型处理
      * */
    val dataStream: DataStreamSource[String] = env.addSource(
      new FlinkKafkaConsumer08[String](
        properties.getProperty("input-data-topic"), new SimpleStringSchema(), properties
      )
    )

  }
}
