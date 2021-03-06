package com.urbancomputing.just.kvsearch.app

import com.urbancomputing.just.kvsearch.util.DistanceUtils._
import com.urbancomputing.just.kvsearch.util.OtherUtils._
import com.urbancomputing.just.kvsearch.util.QueryUtils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object QueryApp {
  /**
   * knn query by brute force
   *
   * @return ([(id, distance)], query time, scan times 1)
   */
  def bruteForce(spark: SparkContext, path: String,
                 qsWithIndex: Seq[(Int, Float)], k: Int): (Seq[(Int, Float)], Float, Int) = {
    val tic = System.currentTimeMillis()
    val data = spark.textFile(path)
    val (startTime, endTime) = (qsWithIndex.head._1, qsWithIndex.last._1)
    val qs = qsWithIndex.map(_._2)
    val res = data
      .map(line => {
        val (id, seq) = splitLine(line)
        (id, seq.slice(startTime, endTime + 1))
      })
      .map(idAndSeq => (idAndSeq._1, chebyshevDistance(idAndSeq._2, qs)))
      .takeOrdered(k)(Ordering[(Float, Int)].on(t => (t._2, t._1)))

    val tok = System.currentTimeMillis()
    (res, (tok - tic) / 1000.0F, 1)
  }

  /**
   * knn query by our kv-search
   *
   * @return ([(id, distance)], query time, scan times)
   */
  def kvSearch(hbaseTableName: String, qsWithIndex: Seq[(Int, Float)], k: Int, isBlockFilter: Boolean,
               timeBlockLen: Int, valueBlockLen: Int, sampleBlockRdd: RDD[(Float, Float)],
               sampleNum: Int, totalNum: Int): (Seq[(Int, Float)], Float, Int) = {
    val tic = System.currentTimeMillis()
    val tuple3 = genTuple3(qsWithIndex, timeBlockLen, valueBlockLen)
    val estimatedDelta = estimateDelta(sampleBlockRdd,
      (qsWithIndex.maxBy(_._2)._2, qsWithIndex.minBy(_._2)._2), k, sampleNum, totalNum)
    val res1 = multiRowRangeQuery(hbaseTableName, tuple3, k, estimatedDelta, isBlockFilter)
    val realDelta = res1.map(_._2).max
    println(
      s"""
         |+++++++
         |estimate delta and res1: [$estimatedDelta, ${res1.length}]
         |real delta in res1:      [$realDelta]
         |+++++++
         |""".stripMargin)

    if (realDelta <= estimatedDelta) {
      (res1, (System.currentTimeMillis() - tic) / 1000.0F, 1)
    } else {
      val res2 = multiRowRangeQuery(hbaseTableName, tuple3, k, realDelta, isBlockFilter)
      (res2, (System.currentTimeMillis() - tic) / 1000.0F, 2)
    }
  }
}
