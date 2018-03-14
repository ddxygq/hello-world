package com.sparkDemo
import org.ansj.splitWord.analysis.{BaseAnalysis, NlpAnalysis, ToAnalysis}

/**
  * Created by Administrator on 2018/2/5.
  */
object WordCountChinese {
  def main(args: Array[String]): Unit = {
    val str = "我是柯广！"
    val parse = BaseAnalysis.parse(str)
    println(parse.toString("\t"))
    var parse2 = ToAnalysis.parse(str)
    println(parse2.toString("\t"))
  }
}
