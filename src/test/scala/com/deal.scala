package com

import org.apache.rocketmq.common.message.MessageExt
import org.apache.spark.rdd.RDD

/**
  * Created by Admin on 2017-08-04.
  */
object deal {
  def process(rdd:RDD[MessageExt]): Unit ={
    println("scala")
    rdd.map(line=>{
      new String(line.getBody)
    }).collect().foreach(println)
    println("scala")
  }
}
