package com.ijunhai

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Admin on 2017-07-31.
  */
object hello_spark {
  def main(args: Array[String]) {
    val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
    val sc=new SparkContext(sparkConf)
    val rdd=sc.parallelize(Array(1,2,3))
    println(rdd.count())
  }
}
