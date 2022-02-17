package com.urbancomputing.just.kvsearch.exp

import com.urbancomputing.just.kvsearch.util.DistanceUtils.chebyshevDistance
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

object ExpUtils {

  private val conf = new SparkConf().setAppName("ExpUtil").setMaster("local[*]")
  private val spark = new SparkContext(conf)

  /**
   * 流式读取原始时序数据
   */
  def readDataToSeq(filePath: String): Iterator[Seq[Double]] = {
    val source = Source.fromFile(filePath, "UTF-8")
    val data = source.getLines()
      .map(line => line.split("\t").last.split(",").map(_.toDouble).toSeq)
    data
  }

  /**
   * 仅保存原始时序的最大最小值数据（仅使用一次）
   */
  def saveBlockData(): Unit = {
    val source = Source.fromFile("E:\\yuzisheng\\data\\ts_185220_6060.txt", "UTF-8")
    // 流式读取处理和保存已缓解内存压力
    val data = source.getLines()
      .map(line => line.split("\t").last.split(",").map(_.toDouble).toSeq)
    val block = data.map(seq => (seq.max, seq.min))
    val writer = new PrintWriter(new File("E:\\yuzisheng\\data\\block_185220_6060.txt"))
    var i = 1
    for (b <- block) {
      writer.write(s"${b._1} ${b._2}\n")
      if (i % 10000 == 0) println(s"save $i/185220 block")
      i += 1
    }
    writer.close()
  }

  /**
   * 读取时序块数据
   */
  def readBlockData(filePath: String): Seq[(Double, Double)] = {
    val source = Source.fromFile(filePath, "UTF-8")
    val blockSeq = source.getLines().map(line => {
      val block = line.split(" ").map(_.toDouble)
      (block.head, block.last)
    }).toSeq
    blockSeq
  }

  /**
   * 模拟HBase中数据的存储格式（仅使用一次）
   */
  def saveHBaseData(tp: Int, tp2: Int): Unit = {
    spark.textFile("E:\\yuzisheng\\data\\ts_185220_6060.txt")
      .flatMap(line => {
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
      .coalesce(1).saveAsTextFile(s"E:\\yuzisheng\\data\\hbase_185220_6060_${tp}_$tp2.txt")
  }

  /**
   * 采样时序块数据并保存（仅使用一次）
   */
  def saveSampleData(): Unit = {
    val percentages = 1 to 100 by 1
    val blockDataRdd = spark.textFile("E:\\yuzisheng\\data\\block_185220_6060.txt")
    for (p <- percentages) {
      val sampleBlocks = blockDataRdd.sample(false, p / 100.0).collect()
      val writer = new PrintWriter(new File(s"E:\\yuzisheng\\data\\sample\\sample_185220_6060_$p.txt"))
      writer.write(sampleBlocks.mkString("\n"))
      writer.close()
    }
  }

  /**
   * 统一读取采样块数据并以数组形式保存至内存
   */
  def getSampleBlocks: Seq[Seq[(Double, Double)]] = {
    val percentages = 1 to 100 by 1
    val sampleDataBlocks = new Array[Seq[(Double, Double)]](100)
    for (p <- percentages) {
      val sampleDataBlock = readBlockData(s"E:\\yuzisheng\\data\\sample\\sample_185220_6060_$p.txt")
      sampleDataBlocks(p - 1) = sampleDataBlock
    }
    sampleDataBlocks
  }

  /**
   * 使用SPARK预计算并保存采样时序的KNN结果（仅使用一次）
   */
  def saveKnnResult(sampleNum: Int): Unit = {
    val ks = 100 to 10000 by 100
    val data = spark.textFile("E:\\yuzisheng\\data\\ts_185220_6060.txt")
      .map(line => line.split("\t").last.split(",").map(_.toDouble))
    val querySeqs = data.takeSample(false, sampleNum)

    val writer = new PrintWriter(new File(s"E:\\yuzisheng\\data\\knn_185220_6060_$sampleNum.txt"))
    querySeqs.par.foreach(querySeq => {
      val topk = data.map(seq => chebyshevDistance(seq, querySeq)).takeOrdered(ks.last)
      val deltas = for (k <- ks) yield topk(k - 1)
      val r = (querySeq.mkString(" "), ks.mkString(" "), deltas.mkString(" "))
      writer.write(s"${r._1}\t${r._2}\t${r._3}" + "\n")
    })
    writer.close()
  }

  /**
   * 流式预计算并保存采样时序的KNN结果（仅使用一次）
   */
  def saveKnnResult2(sampleNum: Int): Unit = {
    def findKMax(arr: Iterator[Double], k: Int): Seq[Double] = {
      val topk = new Array[Double](k)
      for (i <- topk.indices) {
        topk(i) = Double.MaxValue
      }
      while (arr.hasNext) {
        val value = arr.next()
        var maxi = 0
        var maxv = topk(0)
        for (i <- topk.indices) {
          if (topk(i) > maxv) {
            maxi = i
            maxv = topk(i)
          }
        }
        if (value < maxv) {
          topk(maxi) = value
        }
      }
      topk.sorted
    }

    val ks = 100 to 10000 by 100
    val sampleIndices = new ListBuffer[Int]()
    while (sampleIndices.length != sampleNum) {
      val i = Random.nextInt(185220)
      if (!sampleIndices.contains(i)) {
        sampleIndices.append(i)
      }
    }
    println(sampleIndices)

    val querySeqs = new ListBuffer[Seq[Double]]()
    val data = readDataToSeq("E:\\yuzisheng\\data\\ts_185220_6060.txt")
    var i = 0
    while (data.hasNext && querySeqs.length != sampleNum) {
      if (sampleIndices.contains(i)) {
        querySeqs.append(data.next())
      } else {
        data.next()
      }
      i += 1
    }
    println("+++ sample query seq done")

    val writer = new PrintWriter(new File(s"E:\\yuzisheng\\data\\knn_185220_6060_$sampleNum.txt"))
    querySeqs.par.foreach(querySeq => {
      val ts = readDataToSeq("E:\\yuzisheng\\data\\ts_185220_6060.txt")
      val dists = ts.map(seq => chebyshevDistance(seq, querySeq))
      val topk = findKMax(dists, ks.last)
      val deltas = for (k <- ks) yield topk(k - 1)
      val r = (querySeq.mkString(" "), ks.mkString(" "), deltas.mkString(" "))
      writer.write(s"${r._1}\t${r._2}\t${r._3}" + "\n")
    })
    writer.close()
  }

  /**
   * 读取预计算的KNN结果
   */
  def readKnnData(filePath: String): Seq[(Seq[Double], Seq[Int], Seq[Double])] = {
    val source = Source.fromFile(filePath, "UTF-8")
    source.getLines().map(line => {
      val r = line.split("\t")
      val seq = r.head.split(" ").map(_.toDouble).toSeq
      val ks = r(1).split(" ").map(_.toInt).toSeq
      val deltas = r.last.split(" ").map(_.toDouble).toSeq
      (seq, ks, deltas)
    }).toSeq
  }

  /**
   * 预计算原始时序在不同时间周期下的块表示并保存（仅使用一次）
   */
  def saveBlockDataByTp(): Unit = {
    val tps = 100 to 6100 by 100
    tps.par.foreach(tp => {
      println(s"+++ tp = $tp")
      val writer = new PrintWriter(new File(s"E:\\yuzisheng\\data\\block\\block_185220_6060_tp_$tp.txt"))
      val ts = readDataToSeq("E:\\yuzisheng\\data\\ts_185220_6060.txt")
      ts.foreach(seq => {
        val blockSeq = for (i <- seq.indices by tp)
          yield {
            val subSeq = seq.slice(i, i + tp)
            (i / tp, subSeq.max, subSeq.min)
          }
        writer.write(blockSeq.map(r => s"${r._2} ${r._3}").mkString(",") + "\n")
      })
      writer.close()
    })
  }

  def main(args: Array[String]): Unit = {
    saveKnnResult2(1000)
    println("ok")
  }

}
