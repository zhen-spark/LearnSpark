package big.data.analyse.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 相关性检验
  * Created by zhen on 2020/6/25.
  */
object MLCorrelation {
  /**
    * 设置日志级别
    */
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Correlation").master("local[2]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      Vectors.sparse(4, Seq((0, 7.0), (3, -2.0))), // 稀疏向量，等价于dense(7.0, 0.0, 0.0, -2.0)
      Vectors.dense(0.0, 5.0, 3.0, 3.0), // 稠密向量
      Vectors.dense(0.0, 7.0, 5.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    /**
      * NaN ： not a number
      */
    val df = data.map(Tuple1.apply).toDF("features")
    df.show(false)
    val Row(coeff : Matrix) = Correlation.corr(df, "features").head // 使用默认的Pearson
    println(coeff)

    /**
      * 对于Spearman，是排名相关性，我们需要为每个列创建一个RDD [Double]并对其进行排序，以便检索排名，
      * 然后将这些列重新连接到RDD [Vector]中，这是相当昂贵的。 在使用`method =“ spearman”`调用corr之前，
      * 请缓存输入数据集，以避免重新计算公共谱系。
      */
    val Row(coeff2 : Matrix) = Correlation.corr(df, "features", "spearman").head // 指定相关类型
    println(coeff2)

    spark.stop()
  }
}
