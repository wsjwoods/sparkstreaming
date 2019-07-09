package com.woods.ssc

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object SocketWordCount  {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("SocketWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    /** 如果使用了带状态的算子必须要设置checkpoint，
      * 这里是设置在HDFS的文件夹中
      */
    //ssc.checkpoint("hdfs://10.16.66.33:50070/testdata/sparkstreaming/hdfswordcount/")

    val lines = ssc.socketTextStream("10.16.66.33", 6789)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}