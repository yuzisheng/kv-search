package com.urbancomputing.just.kvsearch.app

import com.urbancomputing.just.kvsearch.util.OtherUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DataProcessApp {
  /**
   * check whether the feature dimensions of ts are equal
   */
  def checkData(rdd: RDD[String]): Unit = {
    val columnNum = rdd.first().split("\t").last.split(",").length
    val ts = rdd.map(line => {
      val seq = line.split("\t").last.split(",").map(_.toFloat)
      if (seq.length != columnNum) {
        throw new IllegalArgumentException(s"the dimension of each row are not all the same: " +
          s"$line -> [${seq.length} != $columnNum]")
      }
      (seq.max, seq.min)
    })
    val (globalMax, globalMin) = (ts.map(_._1).max(), ts.map(_._2).min())
    println(
      s"""
         |+++++++
         |check data:
         |shape:       [${rdd.count()}, $columnNum];
         |max and min: [$globalMax, $globalMin]
         |+++++++
         |""".stripMargin)
  }

  /**
   * convert data to [time, time block max, time block min, id, time value block max min, time block]
   *
   * @param rdd           rdd of string
   * @param timeBlockLen  length of time block
   * @param valueBlockLen length of value block in time block
   */
  def convertToTimeBlockRdd(rdd: RDD[String], timeBlockLen: Int,
                            valueBlockLen: Int): RDD[(Int, Float, Float, Int, Seq[(Float, Float)], Seq[Float])] = {
    rdd.flatMap(line => {
      val idAndSeq = line.split("\t", 2)
      val id = idAndSeq.head.toInt
      val seq = idAndSeq.last.split(",").map(_.toFloat).toSeq
      for (i <- seq.indices by timeBlockLen)
        yield {
          val timeBlockSeq = seq.slice(i, i + timeBlockLen)
          val timeValueBlockMaxMin = (for (j <- timeBlockSeq.indices by valueBlockLen)
            yield {
              val timeValueBlock = timeBlockSeq.slice(j, j + valueBlockLen)
              (timeValueBlock.max, timeValueBlock.min)
            }).toArray.toSeq
          // local max-min in time block or global max-min in seq
          // decision: local info can cope with increasing dims of seq
          (i / timeBlockLen, timeBlockSeq.max, timeBlockSeq.min, id, timeValueBlockMaxMin, timeBlockSeq)
        }
    })
  }

  /**
   * linear interpolation to fix missing values
   *
   * @param seq          seq
   * @param missingValue missing value
   * @return (repairable, fixed sequence)
   */
  def linearInterp(seq: Seq[Float], missingValue: Float = 0F): (Boolean, Seq[Float]) = {
    val missingNum = seq.count(_ == missingValue)
    missingNum match {
      case 0 => (true, seq)
      case _ if missingNum == seq.length - 1 =>
        val value = seq.filter(_ != 0).head
        (true, for (_ <- seq.indices) yield value)
      case _ if missingNum == seq.length =>
        (false, Seq.empty)
      case _ =>
        val filledSeq = for ((i, value) <- seq.indices.zip(seq))
          yield {
            if (value == missingValue) {
              // linear interpolation
              val f = seq.lastIndexWhere(_ != missingValue, i - 1)
              val b = seq.indexWhere(_ != missingValue, i + 1)
              if (f != -1 && b != -1) {
                ((seq(b) - seq(f)) / (b - f)) * (i - f) + seq(f)
              } else if (f == -1 && b != -1) {
                val b2 = seq.indexWhere(_ != missingValue, b + 1)
                ((seq(b2) - seq(b)) / (b2 - b)) * (i - b) + seq(b)
              } else {
                val f2 = seq.lastIndexWhere(_ != missingValue, f - 1)
                ((seq(f) - seq(f2)) / (f - f2)) * (i - f2) + seq(f2)
              }
            } else value
          }
        (true, filledSeq)
    }
  }

  /**
   * clean data
   */
  def clean(rdd: RDD[String], missingValue: Float = 0F): Unit = {
    val cleanRdd = rdd.map(line => {
      val idAndSeq = line.split("\t", 2)
      val id = idAndSeq.head.toInt
      val seq = idAndSeq.last.split(",").map(_.toFloat).toSeq
      val rs = linearInterp(seq, missingValue)
      (rs._1, id, rs._2)
    }).filter(_._1).map(t => t._2 + "\t" + t._3.mkString(","))
    cleanRdd.repartition(1).saveAsTextFile("./clean")
  }

  /**
   * generate fake data
   *
   * 0: raw data, 1: double raw data, ...
   */
  def generateFakeTs(rdd: RDD[String], sizeMultiple: Int, dimMultiple: Int): RDD[String] = {
    rdd
      .flatMap(line => {
        val seq = line.split("\t").last.split(",").map(_.toFloat).toSeq
        val fakeSeqs = (1 to sizeMultiple).par.map(_ => genOneFakeSeq(seq, genGaussianZeroToOne()))
        fakeSeqs.toList :+ seq
      })
      .map(seq => {
        val fakeSeqs = (1 to dimMultiple).par.map(_ => genOneFakeSeq(seq, genGaussianZeroToOne()))
        seq ++ fakeSeqs.flatten
      })
      .zipWithIndex()
      .map(t => (t._2 + 1) + "\t" + t._1.mkString(","))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataProcessApp")
      .setMaster("local[*]")
    val spark = new SparkContext(conf)

    val inputPath = DataProcessApp.getClass.getClassLoader.getResource("TS_8820_60.txt").getPath
    val rdd = spark.textFile(inputPath)
    checkData(rdd)

    // raw data
    val fakeRdd = generateFakeTs(rdd, 20, 100)
    fakeRdd.coalesce(1)
      .saveAsTextFile(s"./fake_ts_${fakeRdd.count()}_${fakeRdd.first().split("\t").last.split(",").length}")
  }
}
