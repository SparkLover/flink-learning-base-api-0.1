package com.szsk.yulq.test.DataStreamAPI.DataSources.BuiltInSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object collectionSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    目前flink支持从java.until.Collection 和 java.util.Iterator序列中转换成DataStream数据集

/**
  *
  * 注意: 集合内的数据结构必须要一致
  *
  * */

//    通过fromElements从元素集合中创建DataStream数据集
    import org.apache.flink.api.scala._
    val dataStream = env.fromElements(Tuple2(1L,3L),Tuple2(1L,5L),Tuple2(1L,7L),Tuple2(1L,4L),Tuple2(1L,2L))

//    通过fromCollection从数组转创建DataStream数据集
    /**
      *   java 版
      *   String[] elements = new String []{"hello","flink"};
      *   DataStream<String> dataStream = env.fromCollection(Arrays.asList(elements));
      *
      * */
//    将java.util.List转换成DataStream数据集
    /**
      *  java 版
      * List<String> arrayList = new ArrayList<>();
      * arrayList.add("hello","flink")
      * DataStream<String> dataList = env.fromCollection(ArrayList)
      * */

  }

}
