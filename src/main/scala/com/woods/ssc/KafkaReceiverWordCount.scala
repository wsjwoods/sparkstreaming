package com.woods.ssc

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaReceiverWordCount {
  def main(args: Array[String]): Unit = {
    if(args.length != 4){
      System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group> <topices> <numTreads>")
    }

    val Array(zkQuorum,group,topices,numTreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topicMap = topices.split(",").map((_ ,numTreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
//    messages.map(_._2)
//        .flatMap(_.split("|")).map((_,1)).reduceByKey(_+_)
    println(messages.map(_._2))
    ssc.start()
    ssc.awaitTermination()
  }
}
