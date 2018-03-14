package com.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by Administrator on 2018/1/18.
  */
object AcceptData {

  def main(args: Array[String]): Unit = {
    demo
  }

  /**
    * 客户端
    */
  def demo: Unit ={
    //打开接口接收数据
    val sparkSession = SparkSession.builder()
    val conf = new SparkConf().setMaster("local[3]").setAppName("sqlDemo")
    val ssc = new StreamingContext(conf,Seconds(5))
    val lines:ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
//     val lines:ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,"slave3.hadoop:2181"
//     ,"user-behavior-topic-message-consumer-group",Map("hali" -> 1),StorageLevel.MEMORY_ONLY)
    // KafkaUtils.createDirectStream()
    lines.foreachRDD{ rdd =>
      val sqlSession = sparkSession.getOrCreate()

      //拆分单词
      val wordsDataFrame:DataFrame = sqlSession.read.json(rdd.flatMap(x => x.split(" ")))
      // val wordsDataFrame:DataFrame = sqlSession.read.json(rdd.flatMap(x => x._2.split(" ")))
      //取表名
      wordsDataFrame.createOrReplaceTempView("table1")
      //如果本次流进来的有数据，执行查询操作
      if(wordsDataFrame.count() > 0){
        val rows:DataFrame = sqlSession.sql("select _corrupt_record,count(*) from table1 group by _corrupt_record")
        rows.foreach(x => print(x + " "))
      }
      println()
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
