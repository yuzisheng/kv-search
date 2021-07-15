package com.urbancomputing.app

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DataAnalysisApp {
  /**
   * some statistics
   */
  def statistics(data: RDD[String]): Unit = {
    // todo
    val ts = data.map(_.split("\t").last.split(",").map(_.toDouble))
    val tsMaxMin = ts.map(seq => (seq.max, seq.min))
    val tsAvg = ts.map(seq => seq.sum / seq.length)
    tsMaxMin.saveAsTextFile("./ts-max-min")
    tsAvg.saveAsTextFile("./ts-avg")
  }

  /**
   * sample data to cal and save block
   */
  def sampleBlock(data: RDD[String], fraction: Double = 0.01, deltaHdfsPath: String): Unit = {
    data
      .sample(false, fraction)
      .map(_.split("\t").last.split(",").map(_.toDouble))
      .map(seq => (seq.max, seq.min))
      .map(block => block._1 + " " + block._2)
      .saveAsTextFile(deltaHdfsPath)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataAnalysisApp")
      .setMaster("local[*]")
    val spark = new SparkContext(conf)
    val inputPath = DataAnalysisApp.getClass.getClassLoader.getResource("TS_8820_60.txt").getPath
    val data = spark.textFile(inputPath)
  }
}
