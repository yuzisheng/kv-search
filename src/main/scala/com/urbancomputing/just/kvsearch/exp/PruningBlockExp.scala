package com.urbancomputing.just.kvsearch.exp

import com.urbancomputing.just.kvsearch.exp.PredictDeltaExp.predictDelta
import com.urbancomputing.just.kvsearch.exp.ExpUtils.{readBlockData, readDataToSeq, readKnnData}
import com.urbancomputing.just.kvsearch.util.DistanceUtils.multiBlockDistance

import java.io.{File, PrintWriter}

/**
 * 分块剪枝定理实验
 */
object PruningBlockExp {

  private def getBlock(seq: Seq[Double], tp: Int): Seq[(Double, Double)] = {
    for (i <- seq.indices by tp)
      yield {
        val subSeq = seq.slice(i, i + tp)
        (subSeq.max, subSeq.min)
      }
  }

  private def saveBlockPruningRate(k: Int): Unit = {
    val kIndex = k / 100 - 1
    val tps = 100 to 6100 by 100
    println(s"+++ k=$k, tps=${tps.toList}")

    val tsData = readDataToSeq("E:\\yuzisheng\\data\\ts_185220_6060.txt")
    val knnData = readKnnData("E:\\yuzisheng\\data\\knn_185220_6060_1000.txt").take(10)
    val sampleBlockData = readBlockData("E:\\yuzisheng\\data\\sample\\sample_185220_6060_1.txt")

    val filterNums1 = Array.ofDim[Int](knnData.length, tps.length)
    for (i <- knnData.indices) {
      for (j <- tps.indices) {
        filterNums1(i)(j) = 0
      }
    }
    val filterNums2 = Array.ofDim[Int](knnData.length, tps.length)
    for (i <- knnData.indices) {
      for (j <- tps.indices) {
        filterNums2(i)(j) = 0
      }
    }

    var ii = 0
    while (tsData.hasNext) {
      val seq = tsData.next()
      for (i <- knnData.indices) {
        val (querySeq, _, deltas) = knnData(i)
        for (j <- tps.indices) {
          val tp = tps(j)
          val blockSeq = getBlock(seq, tp)
          val queryBlockSeq = getBlock(querySeq, tp)
          val blockSeqDis = multiBlockDistance(blockSeq, queryBlockSeq)
          val trueDelta = deltas(kIndex)
          val predictedDelta = predictDelta(sampleBlockData, (querySeq.max, querySeq.min), k, 0.01)
          // println("+++", trueDelta, predictedDelta, (predictedDelta - trueDelta) / trueDelta)
          if (blockSeqDis > trueDelta) {
            filterNums1(i)(j) += 1
          }
          if (blockSeqDis > predictedDelta) {
            filterNums2(i)(j) += 1
          }
        }
      }
      ii += 1
      if (ii % 100 == 0) println(s"+++ k=$k, $ii/185220")
    }

    val writer = new PrintWriter(
      new File(s"E:\\yuzisheng\\data\\exp\\pruning_by_block\\exp_pruning_by_block_185220_6060_k_$k.txt"))

    for (i <- knnData.indices) {
      val querySeq = knnData(i)._1
      val trueDeltaPruningRate = filterNums1(i).toSeq.map(n => n / 185220.0)
      val predictedDeltaPruningRate = filterNums2(i).toSeq.map(n => n / 185220.0)
      writer.write(s"${tps.mkString(" ")}\t${trueDeltaPruningRate.mkString(" ")}\t${predictedDeltaPruningRate.mkString(" ")}\n")
    }
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val ks = Seq(1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000)
    ks.par.foreach(saveBlockPruningRate)
  }

}
