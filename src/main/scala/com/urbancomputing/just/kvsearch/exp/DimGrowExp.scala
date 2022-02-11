package com.urbancomputing.just.kvsearch.exp

import com.urbancomputing.just.kvsearch.util.DistanceUtils

object DimGrowExp {

  def main(args: Array[String]): Unit = {
    val data = ExpUtil.readDataToSeq("E:\\yuzisheng\\data\\fake_ts_8820_6000.txt").slice(0, 1000).toSeq
    println(data.size, data.head.size)

    // 时序维度增长
    for (dim <- 100 to 6000 by 100) {
      val data2 = data.map(seq => seq.slice(0, dim))
      val querySeq = data2.head
      // 预热
      for (i <- data2.indices) {
        val _ = DistanceUtils.chebyshevDistance(querySeq, data2(i))
      }

      val tic = System.currentTimeMillis()
      // 重复实验次数
      val repeatNum = 100
      for (_ <- 1 to repeatNum) {
        // 扫描全量数据
        for (i <- data2.indices) {
          val _ = DistanceUtils.chebyshevDistance(querySeq, data2(i))
          // val _ = DistanceUtils.euclideanDistance(querySeq, data2(i))
          // val _ = DistanceUtils.manhattanDistance(querySeq, data2(i))
        }
      }
      val tok = System.currentTimeMillis()
      println(dim + "\t" + (tok - tic) / repeatNum)
    }
  }

}
