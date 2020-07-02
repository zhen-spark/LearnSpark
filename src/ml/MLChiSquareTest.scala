package spark2.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2020/6/28.
  */
object MLChiSquareTest {
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
      (0.0, Vectors.dense(1.0, 1.0, 1.0)),
      (0.0, Vectors.dense(1.0, 1.0, 2.0)),
      (0.0, Vectors.dense(1.0, 1.0, 3.0)),
      (1.0, Vectors.dense(1.0, 3.0, 2.0)),
      (1.0, Vectors.dense(1.0, 3.0, 5.0)),
      (1.0, Vectors.dense(1.0, 3.0, 6.0))
    )
    val df = data.toDF("label", "features")
    val chi = ChiSquareTest.test(df, "features", "label")
    chi.show(false)
  }
}
