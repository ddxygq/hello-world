package com.kafka

import java.util.Timer

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2018/1/21.
  */
object GetDataForKafka {
  def main(args: Array[String]): Unit = {
    getData
  }

  def getData(): Unit ={
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafkaStreaming")
    //sparkConf.set("spark.streaming.receiver.writeAheadLog.enable","true")
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))

    val rmessage:ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext,"slave3.hadoop:2181"
      ,"user-behavior-topic-message-consumer-group",Map("myMessage" -> 1),StorageLevel.MEMORY_ONLY)

//    streamingContext.checkpoint("E:\\bigData\\Kafka\\data")
//    val rmessage:ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext,"slave3.hadoop:2181"
//      ,"user-behavior-topic-message-consumer-group",Map("myMessage" -> 1),StorageLevel.MEMORY_AND_DISK_SER)

    rmessage.foreachRDD(rdd => {
      rdd.persist()
      rdd.foreach(print)
      println("-------------------------------------------------")
    })
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
