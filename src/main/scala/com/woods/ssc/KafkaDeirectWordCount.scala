package com.woods.ssc

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDeirectWordCount {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      System.err.println("Usage: KafkaDeirectWordCount <brokers> <topices> ")
      System.exit(1)
    }

    val Array(brokers,topices) = args
    val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)
    val topicsSet = topices.split(",").toSet


    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc
      ,kafkaParams,topicsSet)
//    messages.map(_._2)
//        .flatMap(_.split("|")).map((_,1)).reduceByKey(_+_)
    println(messages.map(_._2))
    ssc.start()
    ssc.awaitTermination()
  }
}
