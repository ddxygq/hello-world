package com.sparkUtils
import org.apache.spark.{SparkConf,SparkContext}
/**
  * Created by Administrator on 2018/1/16.
  */
object SparkUtil {
  /**
    *
    * @param isLocal：传入true表示本地模式
    * @return：SparkContext对象
    */
  def getSc(isLocal:Boolean): SparkContext = {
    val sparkConf = new SparkConf();
    if(isLocal){
      sparkConf.setMaster("local[2]")
    }else{
      sparkConf.setMaster("spark://master1.hadoop:7077")
      sparkConf.set("spark.testing.memory","2147480000")
    }
    sparkConf.setAppName("SparkDemo")

    val sc = new SparkContext(sparkConf)

    //该段代码只在集群jar包模式才起作用
    /*if(!isLocal) {
      sc.addJar("/home/hadoop/temp/sparkFilterDemo.jar")
    }*/

    return sc
  }
}
