package com.szsk.yulq.test.wordcount

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingWindowWatermarkWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置EventTime作为Flink的时间处理标准，不指定默认为ProcessTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(1)

    //指定Source
    val sourceDS: DataStream[String] = env.socketTextStream("localhost",9000)

    //过滤非法数据 (23432,aa)
    val filterDS: DataStream[String] = sourceDS.filter(new FilterFunction[String] {
      override def filter(t: String): Boolean = {
        if (null == t || "".equals(t)) {
          return false
        }
        val lines = t.split(",")
        if (lines.length != 2) {
          return false
        }
        return true
      }
    })
    import org.apache.flink.streaming.api.scala._
    /*做了一个简单的map转换，将数据转换成Tuple2<long,String,Integer>格式，
        第一个字段代表是时间 第二个字段代表的是单词,第三个字段固定值出现了1次*/
    val mapDS: DataStream[(Long, String, Integer)] = filterDS.map(new MapFunction[String, Tuple3[Long, String, Integer]] {
      override def map(t: String): (Long, String, Integer) = {
        val lines = t.split(",")
        return new Tuple3[Long, String, Integer](lines(0).toLong, lines(1), 1)
      }
    })
    /*设置Watermark的生成方式为Periodic Watermark，并实现他的两个函数getCurrentWatermark和extractTimestamp*/
    val wordcountDS: DataStream[(Long, String, Integer)] = mapDS.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[(Long, String, Integer)] {
        var currentMaxTimestamp = 0L
        //最大允许的消息延迟是5000ms
        val maxoutOfOrderness = 5000L

        override def getCurrentWatermark: Watermark = {
          return new Watermark(currentMaxTimestamp - maxoutOfOrderness)
        }

        override def extractTimestamp(t: (Long, String, Integer), l: Long): Long = {
          val timestamp = t._1
          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
          return timestamp
        }
      }
      /*这里根据第二个元素  单词进行统计 时间窗口是30秒  最大延时是5秒，统计每个窗口单词出现的次数*/
      //注意： Flink的时间窗口是左闭右开的[0,30000)
    ).keyBy(1)
      // 时间窗口是30s
      .timeWindow(Time.seconds(30))
      .sum(2)
    wordcountDS.print("\n 单词统计:")

    env.execute("Window WordCount")


  }
}
