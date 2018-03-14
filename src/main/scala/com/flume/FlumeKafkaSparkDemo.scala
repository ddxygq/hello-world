package com.flume

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2018/3/1.
  */
object FlumeKafkaSparkDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("flume-kafka-spark").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    sparkContext.setCheckpointDir("D:/checkpoints")
    sparkContext.setLogLevel("ERROR")
    val sparkStreaming = new StreamingContext(sparkContext,Seconds(5))

    val topics = Map[String,Int]("test" -> 0)

    val lines = KafkaUtils.createStream(sparkStreaming,"192.168.112.212:2181","test",topics).map(x => x._2)

    val ds1 = lines.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => x + y)
    ds1.print()
    sparkStreaming.start()
    sparkStreaming.awaitTermination()
  }
}
