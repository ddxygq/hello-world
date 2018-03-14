package com.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.spark_project.guava.eventbus.Subscribe

/**
  * Created by Administrator on 2018/2/24.
  */
object KafkaCreateDirectStream {

  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafkaStreaming")
  val streamingContext = new StreamingContext(sparkConf,Seconds(5))

}
