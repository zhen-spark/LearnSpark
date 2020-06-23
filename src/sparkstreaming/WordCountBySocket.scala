package spark2.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时统计
  * Created by Administrator on 2019/10/27.
  */
object WordCountBySocket {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    /**
      * 5s计算一次
      */
    val ssc = new StreamingContext(sc, Seconds(5))

    /**
      * 加载socket数据
      */
    val socketDStream = ssc.socketTextStream("master", 9999)

    val words = socketDStream
      .flatMap(_.split(" "))
      .map(item => (item, 1))
      .filter(_._1.toLowerCase != "flink")
      .reduceByKey(_ + _)

    words.print()
    words.repartition(1) // 重新分区
      .count()
      .print()

    /**
      * 开启流计算
      */
    ssc.start()
    ssc.awaitTermination()
  }
}
