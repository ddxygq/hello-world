package com.flume

import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2018/3/1.
  */
object FlumePushWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("flume-push-spark").setMaster("local[2]")
    val sparkStreaming = new StreamingContext(conf,Seconds(5))

    // 本机地址
    val flumeStream = FlumeUtils.createStream(sparkStreaming,"192.168.1.50",8888)
    val words = flumeStream.flatMap(x => new String(x.event.getBody.array()).split(" ").map(x => (x,1)))
    val results = words.reduceByKey((x,y) => x + y)
    results.print()
    sparkStreaming.start()
    sparkStreaming.awaitTermination()
  }
}
