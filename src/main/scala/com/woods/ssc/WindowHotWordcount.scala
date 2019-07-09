/**
  * 窗口滑动
  ** 基于滑动窗口的热点搜索词实时统计
  */
object WindowHotWordcount{
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("UpdateStateByKey").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))


    val searchLogsDsteam = ssc.socketTextStream("10.22.66.1", 6789)

    val searchWordsDsteam = searchLogsDsteam.map(_.split(" ")(1)).map((_,1))

    // 针对(searchWord, 1)的tuple格式的DStream，执行reduceByKeyAndWindow，滑动窗口操作
    // 第二个参数，是窗口长度，这里是60秒
    // 第三个参数，是滑动间隔，这里是10秒
    // 也就是说，每隔10秒钟，将最近60秒的数据，作为一个窗口，进行内部的RDD的聚合，然后统一对一个RDD进行后续
    // 计算
    // 所以说，这里的意思，就是，之前的searchWordPairDStream为止，其实，都是不会立即进行计算的
    // 而是只是放在那里
    // 然后，等待我们的滑动间隔到了以后，10秒钟到了，会将之前60秒的RDD，因为一个batch间隔是，5秒，所以之前
    // 60秒，就有12个RDD，给聚合起来，然后，统一执行redcueByKey操作
    // 所以这里的reduceByKeyAndWindow，是针对每个窗口执行计算的，而不是针对某个DStream中的RDD
    val searchWordCounts = searchWordsDsteam.reduceByKeyAndWindow((v1:Int,v2:Int) => v1+v2,Seconds(60),Seconds(10))

    val finalDStream = searchWordCounts.transform( wordRDD => {
      val countRDD = wordRDD.map( touple => (touple._2,touple._1)).sortByKey(false).map( touple => (touple._2,touple._1)).take(3)
      for(word <- countRDD) {
        println(word)
      }
      wordRDD
    })

    finalDStream.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
