package com.szsk.yulq.test.wordcount

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark

object MainWaterMark001 {
  def main(args: Array[String]): Unit = {
    //1.准备运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2.设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //3.source
    val SingleOutputStreamOpeartor: DataStream[String] = env.socketTextStream("localhost", 12345)
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[String] {
        //当前时间戳
        var currentTimeStamp = 0L
        // 允许的迟到数据
        var maxDelayAllowed = 0L
        //当前水位线
        var currentWaterMark = 0L

        override def getCurrentWatermark: Watermark = {
          currentWaterMark = currentTimeStamp - maxDelayAllowed
          System.out.println("当前水位线:" + currentWaterMark)
          return new Watermark(currentWaterMark)
        }

        override def extractTimestamp(t: String, l: Long): Long = {
          val arr: Array[String] = t.split(",")
          val timeStamp= arr(1).toLong
          currentTimeStamp = Math.max(timeStamp, currentTimeStamp)
          System.out.println("Key:" + arr(0) + ",EventTime:" + timeStamp + ",水位线:" + currentWaterMark)
          return timeStamp
        }
      })
//    SingleOutputStreamOpeartor.map(new MapFunction[String, Tuple2[String,String]]() {
    //      @throws[Exception]
    //      override def map(s: String) = new (s.split(",")(0), s.split(",")(1))
    //    }).keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //      .fold("start",new FoldFunction[Tuple2[String,String],String] {
    //        override def fold(t: String, o: (String, String)): String = {
    //          return t + "-" +o._1
    //        }
    //      }).print()

    env.execute("MainWaterMark001")

  }

}
