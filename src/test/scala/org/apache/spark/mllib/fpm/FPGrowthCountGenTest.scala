package org.apache.spark.mllib.fpm

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/2/5.
  */
object FPGrowthCountGenTest {

  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _
 var savePath = "H:\\test\\output\\fpg"
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

    val fpg = new FPGrowthCount()

    val model3 = fpg
      .setMinCount(2)
      .setNumPartitions(2)
      .run(rdd)
    val freqItemsets3 = model3.freqItemsets.collect().map { itemset =>
      (itemset.items.toSet, itemset.freq)
    }

    freqItemsets3.foreach(println)


    model3.save(sc, savePath)

    sc.stop()
  }
}