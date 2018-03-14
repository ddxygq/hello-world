package com.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/1/23.
  */
object HiveDemo {
  def main(args: Array[String]): Unit = {
    hiveDemo()
  }

  def hiveDemo(): Unit ={
    val sparkSession = SparkSession.builder()
    sparkSession.master("local").appName("sparkHiveDemo")
    sparkSession.enableHiveSupport()
    val hiveSession = sparkSession.getOrCreate()
    val rows:DataFrame = hiveSession.sql("select * from record")
    val oneRow = rows.first()
    println(oneRow.mkString("\t"))
  }
}
