package com.urbancomputing.just.kvsearch.exp

import com.urbancomputing.just.kvsearch.exp.DeltaPredictExp.predictDelta
import com.urbancomputing.just.kvsearch.exp.ExpUtil.{readBlockData, readKnnData}

import java.io.{File, PrintWriter}

object ExtremumPruningExp {

  private def getBlock(seq: Seq[Double], tp: Int): Seq[(Double, Double)] = {
    for (i <- seq.indices by tp)
      yield {
        val subSeq = seq.slice(i, i + tp)
        (subSeq.max, subSeq.min)
      }
  }

  private def saveExtremumPruningRate(k: Int): Unit = {
    val kIndex = k / 100 - 1
    println(s"+++ start to exp pruning by extremum, k=$k")

    val knnData = readKnnData("E:\\yuzisheng\\data\\knn_185220_6060_1000.txt")
    val blockData = readBlockData("E:\\yuzisheng\\data\\block_185220_6060.txt")
    val sampleBlockData = readBlockData("E:\\yuzisheng\\data\\sample\\sample_185220_6060_1.txt")

    val filterNums1 = new Array[Int](knnData.length)
    for (i <- knnData.indices) filterNums1(i) = 0
    val filterNums2 = new Array[Int](knnData.length)
    for (i <- knnData.indices) filterNums2(i) = 0

    knnData.zipWithIndex.foreach(r => {
      val ((querySeq, _, deltas), i) = r
      val trueDelta = deltas(kIndex)
      val predictedDelta = predictDelta(sampleBlockData, (querySeq.max, querySeq.min), k, 0.01)
      // println("+++", trueDelta, predictedDelta, (predictedDelta - trueDelta) / trueDelta)
      val keyRange1 = (querySeq.min - trueDelta, querySeq.max + trueDelta)
      val keyRange2 = (querySeq.min - predictedDelta, querySeq.max + predictedDelta)
      blockData.foreach(b => {
        val seqMax = b._1
        if (seqMax < keyRange1._1 || seqMax > keyRange1._2) filterNums1(i) += 1
        if (seqMax < keyRange2._1 || seqMax > keyRange2._2) filterNums2(i) += 1
      })
    })

    val writer = new PrintWriter(
      new File(s"E:\\yuzisheng\\data\\exp\\pruning_by_extremum\\exp_pruning_by_extremum_185220_6060_k_$k.txt"))

    for (i <- knnData.indices) {
      val querySeq = knnData(i)._1
      val trueDeltaPruningRate = filterNums1(i) / 185220.0
      val predictedDeltaPruningRate = filterNums2(i) / 185220.0
      writer.write(s"$trueDeltaPruningRate\t$predictedDeltaPruningRate\n")
    }
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    // val ks = Seq(1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000)
    val ks = 100 to 10000 by 100
    ks.foreach(saveExtremumPruningRate)
  }

}
