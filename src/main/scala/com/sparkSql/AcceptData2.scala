package com.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.ansj.splitWord.analysis.{ToAnalysis,DicAnalysis}
import org.ansj.recognition.impl.StopRecognition
import org.nlpcn.commons.lang.tire.library

/**
  * Created by Administrator on 2018/1/18.
  */
object AcceptData2 {

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
    // val lines:ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val lines:ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,"slave3.hadoop:2181"
      ,"user-behavior-topic-message-consumer-group",Map("hali" -> 1),StorageLevel.MEMORY_ONLY)

    // 停词
   /* val stopWord = new StopRecognition()
    stopWord.insertStopNatures("w") // 过滤掉标点
    stopWord.insertStopNatures("m") // 过滤掉m词性
    stopWord.insertStopNatures("null") // 过滤掉null词性
    stopWord.insertStopNatures("<br />") // 过滤掉<br />词性
    stopWord.insertStopNatures(":")
    stopWord.insertStopNatures("'")*/
    lines.foreachRDD{ rdd =>
      val sqlSession = sparkSession.getOrCreate()

      println("==================================================================")
      rdd.foreach(x =>
        println(x)
      )
      //拆分单词
      //val word = rdd.flatMap(x => x._2)
      //var words = word.map(x => ToAnalysis.parse(x))
     // println(words)
     // val wordsDataFrame:DataFrame = sqlSession.read.json(words)

     // println(wordsDataFrame)

      //println("结果" + words.toString())
      // val wordsDataFrame:DataFrame = sqlSession.read.json(rdd.flatMap(x => x._2.split(",")))


      //val wordSplit = words.map(x => ToAnalysis.parse(x))
      //wordSplit.foreach(println)
       //取表名
       // wordsDataFrame.createOrReplaceTempView("table1")
      // 如果本次流进来的有数据，执行查询操作
      /*if(wordsDataFrame.count() > 0){
        val rows:DataFrame = sqlSession.sql("select _corrupt_record,count(*) from table1 group by _corrupt_record")
        rows.foreach(x => print(x + " "))
      }*/
      // println()
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
