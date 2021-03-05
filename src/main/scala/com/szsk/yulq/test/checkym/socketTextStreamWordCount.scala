package com.szsk.yulq.test.checkym

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object socketTextStreamWordCount {
  def main(args: Array[String]) = {
    if(args.length != 2){
      System.err.println("USAGE:\nSocketTextStreamWordCount<hostname><port>")
    }
    val hostName = args(0)
    val port = args(1).toInt
    //set up the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  //隐式转换 appLy直接调用伴生对象
    implicitly  val f = (x:Int) => x +1

    // get input data
   val text: DataStream[String] = env.socketTextStream(hostName,port)
    import org.apache.flink.streaming.api.scala._
    text.flatMap(new LineSplitter()).setParallelism(1)
      .keyBy(0)
      .sum(1)
      .setParallelism(1)
      .print()
    env.execute("Scala WordCount from SocketTextStream Example")


  }

  class LineSplitter extends FlatMapFunction[String,Tuple2[String,Integer]]{
    override def flatMap(value: String, out: Collector[Tuple2[String, Integer]]): Unit = {
      val tokens: Array[String] = value.toLowerCase.split("\\W+")
      for(token <- tokens){
        if(tokens.length>0){
          out.collect(new Tuple2[String,Integer](token,1))
        }
      }
    }
  }



}
