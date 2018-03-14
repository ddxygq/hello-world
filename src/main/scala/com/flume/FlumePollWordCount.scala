package com.flume

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

/**
  * Created by Administrator on 2018/3/1.
  */
object FlumePollWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("flume-poll-spark").setMaster("local[2]")
    val sparkStreaming = new StreamingContext(conf,Seconds(5))

    val address = Seq(new InetSocketAddress("192.168.112.134",8888))
    val flumeStream = FlumeUtils.createPollingStream(sparkStreaming,address,StorageLevel.MEMORY_ONLY)
    val words = flumeStream.flatMap(x => new String(x.event.getBody.array()).split(" ").map(x => (x,1)))
    val results = words.reduceByKey((x,y) => x + y)
    results.print()
    sparkStreaming.start()
    sparkStreaming.awaitTermination()
  }
}
