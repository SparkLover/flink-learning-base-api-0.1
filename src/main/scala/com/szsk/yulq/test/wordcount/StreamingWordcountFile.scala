package com.szsk.yulq.test.wordcount

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamingWordcountFile {
  def main(args: Array[String]): Unit = {
    //设定执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //指定数据源地址，读取输入数据
    val text: DataStream[String] = env.readTextFile("file:///path/file")
    //对数据集指定转换操作逻辑
    import org.apache.flink.streaming.api.scala._
    val counts: DataStream[(String, Int)] = text.flatMap(_.toLowerCase.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)
    //指定计算结果输出位置
    counts.writeAsText("filepath")
    //指定名称并触发流式任务
    env.execute("streaming file wordcount")

//    匿名内部类
    text.map( new MapFunction[String,String] {
      override def map(value: String): String = value.toUpperCase()
    })
//  自定义类
    class  MymapFuncation extends  MapFunction[String,String]{
      override def map(t:String): String = {
        t.toUpperCase()
      }
    case class Person(name:String,age:Int)
  private val pp: DataStream[Person] = env.fromElements(Person("hello",1),Person("hi",1))
//  定义keyselector ，实现getkey方法从case class 中获取key

    }
  }


}
