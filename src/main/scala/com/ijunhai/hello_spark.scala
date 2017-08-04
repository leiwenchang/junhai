package com.ijunhai

import com.ip_analysis.IP
import com.ip_analysis.IP._
import org.apache.rocketmq.spark.{ConsumerStrategy, LocationStrategy, RocketMQConfig, RocketMqUtils}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{JavaConverters, mutable}

/**
  * Created by Admin on 2017-07-31.
  */
object hello_spark {
  def main(args: Array[String]) {
//    System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
//    val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
//    val sc=new SparkContext(sparkConf)
//
//    val rdd=sc.parallelize(getIPAddress)
//
//    rdd.count()
    System.setProperty("hadoop.home.dir", "E:\\64bit_software\\64bit_software\\hadoop-2.6.0-x64\\hadoop-2.6.0")
    val sparkConf=new SparkConf().setMaster("local").setAppName("hello_spark")
    val optionParam= mutable.HashMap[String,String]()
    optionParam.put(RocketMQConfig.NAME_SERVER_ADDR,"192.168.137.102:9876")
    val ssc=new JavaStreamingContext(sparkConf,Seconds(1))
    val topic=List("hello")
//    val data=ssc.socketTextStream("127.0.0.1",9527)
//    data.foreachRDD(line=>{
//      line.collect().foreach(println)
//    })
    val stream=RocketMqUtils.createJavaMQPullStream(ssc,"TagA",JavaConverters.seqAsJavaListConverter(topic).asJava,ConsumerStrategy.earliest,
      false,false,false,LocationStrategy.PreferConsistent,JavaConverters.mapAsJavaMapConverter(optionParam).asJava)
//    val dStream = RocketMqUtils.createMQPullStream(ssc, "TagA", "hello", ConsumerStrategy.earliest, true, false, false,JavaConverters.mapAsJavaMapConverter(optionParam).asJava)
//    dStream.map(message => message.getBody).print()
    stream.print()
    println("hello")
    ssc.start()
    ssc.awaitTermination()
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
