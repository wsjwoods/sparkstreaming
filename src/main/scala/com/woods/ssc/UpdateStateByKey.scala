object UpdateStateByKey{
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("UpdateStateByKey").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    /** 如果使用了带状态的算子必须要设置checkpoint，
      * 这里是设置在HDFS的文件夹中
      */
    ssc.checkpoint("hdfs://10.16.66.33:8020/testdata/sparkstreaming/hdfswordcount/")

    val lines = ssc.socketTextStream("10.16.66.33", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1))

    val state = result.updateStateByKey[Int](updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 把当前的数据去更新已有的数据
    * currentValues: Seq[Int] 新有的状态
    * preValue: Option[Int] 已有的状态
    **/
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val currentCount = currentValues.sum
    //每个单词出现了多少次
    val preCount = preValues.getOrElse(0)

    Some(currentCount + preCount) //返回


  }
}
