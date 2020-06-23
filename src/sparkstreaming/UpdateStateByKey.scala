package spark2.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2020/5/7.
  */
object UpdateStateByKey {
  /**
    * 累加
    * @param newValues  当前周期新数据的值
    * @param runningCount  历史累计值
    * @return
    */
  def updateFunction(newValues : Seq[Int], runningCount : Option[Int]) : Option[Int] = {
    val preCount = runningCount.getOrElse(0)
    val newCount = newValues.sum
    Some(newCount + preCount) // 累加
  }

  Logger.getLogger("org").setLevel(Level.WARN) // 设置日志级别
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName(s"${this.getClass.getSimpleName}")
    val ssc = new StreamingContext(conf,Seconds(5))
    // 设置检测点
    ssc.checkpoint("E:\\checkpoint")
    val lines = ssc.socketTextStream("master",9999) // 与nc端口对应
    val words = lines.flatMap(_.split(" "))
    var pairs = words.map(word=>(word,1)).reduceByKey(_+_)
    // 累加
    pairs = pairs.updateStateByKey[Int](updateFunction _) // 必须设置检查点
    pairs.foreachRDD(row => row.foreach(println))
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
