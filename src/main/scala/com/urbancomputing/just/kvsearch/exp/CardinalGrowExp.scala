package com.urbancomputing.just.kvsearch.exp

import com.urbancomputing.just.kvsearch.exp.ExpUtil.readDataToSeq
import com.urbancomputing.just.kvsearch.util.DistanceUtils

object CardinalGrowExp {

  def main(args: Array[String]): Unit = {
    val data = readDataToSeq("E:\\yuzisheng\\data\\ts_8820_6000.txt").slice(0, 1000).toSeq
    println(data.size, data.head.size)

    // 数据基数增长（百分比）
    for (size <- 1 to 100) {
      val data2 = data.slice(0, (data.size * size / 100.0).toInt)
      val querySeq = data2.head
      // 预热
      for (i <- data2.indices) {
        val _ = DistanceUtils.chebyshevDistance(querySeq, data2(i))
      }

      val tic = System.currentTimeMillis()
      // 重复实验次数
      val repeatNum = 20
      for (_ <- 1 to repeatNum) {
        // 扫描全量数据
        for (i <- data2.indices) {
          val _ = DistanceUtils.chebyshevDistance(querySeq, data2(i))
          // val _ = DistanceUtils.euclideanDistance(querySeq, data2(i))
          // val _ = DistanceUtils.manhattanDistance(querySeq, data2(i))
        }
      }
      val tok = System.currentTimeMillis()
      println(size + "\t" + (tok - tic) / repeatNum)
    }
  }

}
