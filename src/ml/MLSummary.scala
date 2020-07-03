package spark2.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.stat.Summarizer._
/**
  * Created by Administrator on 2020/7/3.
  */
object MLSummary {
  /**
    * 设置日志级别
    */
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.driver.maxResultSize", "2G")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (Vectors.dense(7.0, 3.0, 4.0), 1.0),
      (Vectors.dense(4.0, 6.0, 7.0), 2.0)
    )
    val df = data.toDF("features", "weight")

    /**
      * 带权重的均值、方差计算
      */
    val (meanVal, varianceVal) = df.select(metrics("mean", "variance").summary($"features", $"weight").as("summary"))
      .select("summary.mean", "summary.variance")
      .as[(Vector, Vector)]
      .first()
    println(meanVal)
    println(varianceVal)

    /**
      * 不带权重的均值、方差计算
      */
    val (meanVal2, varianceVal2) = df.select(mean($"features"), variance($"features"))
      .as[(Vector, Vector)]
      .first()
    println(meanVal2)
    println(varianceVal2)

    spark.stop()
  }
}