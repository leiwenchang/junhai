package com.ijunhai

import com.ip_analysis.IP
import com.ip_analysis.IP._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017-07-31.
  */
object hello_spark {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
    val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
    val sc=new SparkContext(sparkConf)

    val rdd=sc.parallelize(getIPAddress)
    rdd.count()
  }
  def getIPAddress: IndexedSeq[Array[String]] ={
    IP.load("E:\\64bit_software\\64bit_software\\17monipdb\\17monipdb.dat")
    val st: Long = System.nanoTime
    var i: Int = 0
    val result=for (i<- 0 until 1000000) yield{
      IP.find(randomIp)
    }
    val et: Long = System.nanoTime
    System.out.println((et - st) / 1000 / 1000)
    result
  }
}
