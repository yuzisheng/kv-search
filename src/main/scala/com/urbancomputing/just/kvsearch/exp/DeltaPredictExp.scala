package com.urbancomputing.just.kvsearch.exp

import com.urbancomputing.just.kvsearch.exp.ExpUtil._

import java.io.{File, PrintWriter}
import scala.math._

object DeltaPredictExp {

  /**
   * 找到第k大的值
   */
  private def findKthMax(data: Seq[Double], k: Int): Double = {
    val topk = new Array[Double](k)
    for (i <- topk.indices) {
      topk(i) = Double.MaxValue
    }
    for (value <- data) {
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
    topk.max
  }

  /**
   * 预测delta
   */
  private def predictDelta(sampleBlockData: Seq[(Double, Double)], qsBlock: (Double, Double), k: Int, fraction: Double): Double = {
    val sampleK = (k * fraction).toInt + 1
    val upperDists = sampleBlockData.map(b => max(abs(b._1 - qsBlock._2), abs(b._2 - qsBlock._1)))
    findKthMax(upperDists, sampleK)
  }

  /**
   * 测试delta随alpha变化的趋势：理论上随着采样率alpha的增大，delta预测会更加精确
   */
  def deltaByAlpha(knnData: Seq[(Seq[Double], Seq[Int], Seq[Double])],
                   fileName: String): Unit = {
    val percentages = 1 to 100 by 1
    val ks = 100 to 10000 by 100
    val sampleDataBlocks = getSampleBlocks

    for (k <- ks) {
      println(s"+++ k = $k")
      val deltaByAlphaRes = knnData.par.map(knnRecord => {
        val (querySeq, ks, trueDeltas) = knnRecord
        val queryBlock = (querySeq.max, querySeq.min)
        var trueDelta = trueDeltas.head
        for ((k2, trueDelta2) <- ks.zip(trueDeltas)) {
          if (k2 == k) trueDelta = trueDelta2
        }

        val relativeErrors = percentages.par.map(p => {
          (predictDelta(sampleDataBlocks(p - 1), queryBlock, k, p / 100.0) - trueDelta) / trueDelta
        })
        relativeErrors
      })
      val writer = new PrintWriter(
        new File(s"E:\\yuzisheng\\data\\exp\\delta_by_alpha\\exp_delta_by_alpha_${fileName}_k_$k.txt"))
      writer.write(deltaByAlphaRes.transpose.map(r => r.sum / r.size).mkString("\n"))
      writer.close()
    }
  }

  /**
   * 测试delta随k变化的趋势：理论上随着k的增大，delta预测会更加精确
   */
  def deltaByK(knnData: Seq[(Seq[Double], Seq[Int], Seq[Double])],
               fileName: String): Unit = {
    val percentages = 1 to 100 by 1
    val sampleDataBlocks = getSampleBlocks

    for (p <- percentages) {
      println(s"+++ fraction = $p/100")
      val sampleBlock = sampleDataBlocks(p - 1)
      val deltaByKRes = knnData.map(knnRecord => {
        val (querySeq, ks, trueDeltas) = knnRecord
        val relativeErrors = ks.zip(trueDeltas).par.map(t => {
          val (k, trueDelta) = t
          (predictDelta(sampleBlock, (querySeq.max, querySeq.min), k, p / 100.0) - trueDelta) / trueDelta
        })
        relativeErrors
      })
      val writer = new PrintWriter(
        new File(s"E:\\yuzisheng\\data\\exp\\delta_by_k\\exp_delta_by_k_${fileName}_alpha_$p.txt"))
      writer.write(deltaByKRes.transpose.map(r => r.sum / r.size).mkString("\n"))
      writer.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val fileName = "185220_6060"
    val knnData = readKnnData("E:\\yuzisheng\\data\\knn_185220_6060_290.txt")
    println("+++ read knn data done")

    deltaByK(knnData, fileName)
    deltaByAlpha(knnData, fileName)
  }

}

