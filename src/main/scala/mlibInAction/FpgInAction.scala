package mlibInAction

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.fpm._

/**
  * Created by Administrator on 2017/2/4.
  */
object FpgInAction {

  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _

  def main(args: Array[String]) {

    spark = SparkSession.builder
      .master("local[2]")
      .appName("MLlibUnitTest")
      .getOrCreate()
    sc = spark.sparkContext

    val transactions = Seq(
      "r z h k p",
      "z y x w v u t s",
      "s x o n r",
      "x z y m t s q e",
      "z",
      "x z y r q t p")
      .map(_.split(" "))
    val rdd = sc.parallelize(transactions, 2).cache()

    val fpg = new FPGrowth()

    val model3 = fpg
      .setMinSupport(0.5)
      .setNumPartitions(2)
      .run(rdd)
    val freqItemsets3 = model3.freqItemsets.collect().map { itemset =>
      (itemset.items.toSet, itemset.freq)
    }

    freqItemsets3.foreach(println)
    sc.stop()
  }
}
