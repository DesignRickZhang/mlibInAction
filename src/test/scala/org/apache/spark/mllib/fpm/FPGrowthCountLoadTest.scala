package org.apache.spark.mllib.fpm

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/2/5.
  */
object FPGrowthCountLoadTest {

  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _

  def main(args: Array[String]) {

    spark = SparkSession.builder
      .master("local[2]")
      .appName("MLlibUnitTest")
      .getOrCreate()
    sc = spark.sparkContext

    val model = FPGrowthModel.load(sc, FPGrowthCountGenTest.savePath)
  model.freqItemsets.cache()



    val freqItemsets3 = model.freqItemsets.collect().map { itemset =>
      (itemset.items.toSet, itemset.freq)
    }

    freqItemsets3.foreach(println)
    val targetItem = "r"
    println(s"containe target:${targetItem}")
    model.freqItemsets.filter(itemset=> itemset.items.contains(targetItem)).collect().map { itemset =>
      (itemset.items.toSet, itemset.freq)
    }.foreach(println)

    sc.stop()
  }
}