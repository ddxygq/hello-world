package com.sparkDemo

import com.sparkUtils.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/1/16.
  */


object wordCount {

  def main(args: Array[String]): Unit = {
    //wordCount

    //wordCountCluster

    filterDemo
  }

  def wordCount(): Unit ={
    val sparkContext = SparkUtil.getSc(true)

    //val lines = sparkContext.textFile("hali2.txt")
    val lines = sparkContext.textFile("hdfs://master1.hadoop:9000/spark/hali2.txt")

    val worcount = lines.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => x + y).take(10).foreach(print)

  }

  //集群模式
  def wordCountCluster(): Unit ={
    val sparkContext = SparkUtil.getSc(false)

    val lines = sparkContext.textFile("hdfs://master1.hadoop:9000/spark/hali2.txt")

    //val worcount = lines.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => x + y).take(10)

    //worcount.foreach(x => println(x + " "))

    println("总行数：" + lines.count())

  }

  def filterDemo(): Unit ={
    val sparkContext = SparkUtil.getSc(false)

    val lines = sparkContext.textFile("hdfs://master1.hadoop:9000/spark/hali2.txt")

    val filterData = lines.filter(x => x.contains("good")).count()

    println(filterData)
  }
}