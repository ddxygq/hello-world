package com.sparkSql

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source
import scala.util.Random

/**
  * Created by Administrator on 2018/1/18.
  */
object SocketSendData {

  def main(args: Array[String]): Unit = {
    demo
  }

  //随机获取整数
  def index(length:Int): Int ={
    val rdm = new Random()
    rdm.nextInt(length)
  }

  def demo(): Unit ={
    val inpath = "E:\\bigData\\spark\\课件\\第10章Spark-Streaming\\logs\\hali.txt"
    val contexts = Source.fromFile(inpath,"gbk").getLines().toList
    println("读取文件内容："+contexts.length)
    val server = new ServerSocket(9999) //打开服务端
    println("服务器已经开放")
    while (true){
      println("等待连接")
      val socket = server.accept()
      println("客户端已连接")
      new Thread(){
        override def run() ={
          while(true){
            val out = new PrintWriter(socket.getOutputStream,true)
            val context = contexts(index(contexts.length))
            println("读取内容："+context)
            out.write(context)
            out.flush()
            println("发送内容完成")
            Thread.sleep(1000)
            out.close()
          }
        }
      }.start()
    }
  }
}
