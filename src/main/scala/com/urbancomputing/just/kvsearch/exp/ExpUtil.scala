package com.urbancomputing.just.kvsearch.exp

import com.urbancomputing.just.kvsearch.util.DistanceUtils.chebyshevDistance
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}
import scala.io.Source

object ExpUtil {

  private val conf = new SparkConf().setAppName("ExpUtil").setMaster("local[*]")
  private val spark = new SparkContext(conf)

  def readDataToSeq(filePath: String): Seq[Seq[Double]] = {
    val source = Source.fromFile(filePath, "UTF-8")
    val data = source.getLines()
      .map(line => line.split("\t").last.split(",").map(_.toDouble).toSeq).toSeq
    data
  }

  def readDataToRdd(filePath: String): RDD[Seq[Double]] = {
    spark.textFile(filePath).map(line => line.split("\t").last.split(",").map(_.toDouble).toSeq)
  }

  def readSampleBlockDataToRdd(filePath: String): RDD[(Double, Double)] = {
    spark.textFile(filePath).map(line => {
      val arr = line.split(" ").map(_.toDouble)
      (arr.head, arr.last)
    })
  }

  def convertData(data: RDD[String], tp: Int, tp2: Int, outputFileName: String): Unit = {
    data.flatMap(line => {
      val idAndSeq = line.split("\t", 2)
      val id = idAndSeq.head.toInt
      val seq = idAndSeq.last.split(",").map(_.toDouble).toSeq
      for (i <- seq.indices by tp)
        yield {
          val timeBlockSeq = seq.slice(i, i + tp)
          val timeValueBlockMaxMin = (for (j <- timeBlockSeq.indices by tp2)
            yield {
              val timeValueBlock = timeBlockSeq.slice(j, j + tp2)
              (timeValueBlock.max, timeValueBlock.min)
            }).toArray.toSeq
          (i / tp, timeBlockSeq.max, timeBlockSeq.min, id, timeValueBlockMaxMin, timeBlockSeq)
        }
    })
      .sortBy(r => (r._1, r._2, r._3, r._4))
      .map(record => {
        val (time, timeBlockSeqMax, timeBlockSeqMin, id, block, seq) = record
        s"$time $timeBlockSeqMax $timeBlockSeqMin $id\t${block.map(t => t._1 + " " + t._2).mkString(",")}\t${seq.mkString(",")}"
      })
      .coalesce(1).saveAsTextFile(s"E:\\yuzisheng\\data\\$outputFileName")
  }

  def sampleData(data: RDD[String], fraction: Double = 0.01, deltaHdfsPath: String): Unit = {
    data
      .sample(false, fraction)
      .map(_.split("\t").last.split(",").map(_.toDouble))
      .map(seq => (seq.max, seq.min))
      .map(block => block._1 + " " + block._2)
      .coalesce(1)
      .saveAsTextFile(deltaHdfsPath)
  }

  def saveKnnResult(rawSeqRdd: RDD[Seq[Double]]): Unit = {
    // val ks = List(10000, 20000, 40000, 50000, 60000, 70000, 80000, 90000, 100000)
    val ks = 100 to 10000 by 100

    val sampleNum = 500
    val querySeqs = rawSeqRdd.takeSample(false, sampleNum)
    println("+++ sample data done")

    val writer = new PrintWriter(new File(s"E:\\yuzisheng\\data\\knn_185220_6060_$sampleNum.txt"))
    querySeqs.par.foreach(querySeq => {
      val topk = rawSeqRdd.map(seq => chebyshevDistance(seq, querySeq)).takeOrdered(ks.last)
      val deltas = for (k <- ks) yield topk(k - 1)
      val r = (querySeq.mkString(" "), ks.mkString(" "), deltas.mkString(" "))
      writer.write(s"${r._1}\t${r._2}\t${r._3}" + "\n")
    })
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    // val data = spark.textFile("E:\\yuzisheng\\data\\ts_185220_6060.txt")
    // convertData(data, 1000, 100, "hbase_table_185220_6060_1000_100")

    // val fList = List(0.6, 0.7, 0.8, 0.9, 1.0)
    // for (f <- fList) {
    //   sampleData(data, f, s"E:\\yuzisheng\\data\\sample_185220_6060_$f")
    //   }

    val data = readDataToRdd("E:\\yuzisheng\\data\\ts_185220_6060.txt")
    data.persist()
    println("+++ read data done")
    saveKnnResult(data)
  }

}
