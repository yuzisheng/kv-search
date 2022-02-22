package com.urbancomputing.just.kvsearch.util

import scala.math._

object DistanceUtils {
  /**
   * euclidean distance
   */
  def euclideanDistance(s1: Seq[Float], s2: Seq[Float]): Float = {
    assert(s1.length == s2.length)
    sqrt(s1.zip(s2).map(p => pow(p._1 - p._2, 2)).sum).toFloat
  }

  /**
   * manhattan distance
   */
  def manhattanDistance(s1: Seq[Float], s2: Seq[Float]): Float = {
    assert(s1.length == s2.length)
    s1.zip(s2).map(p => abs(p._1 - p._2)).sum
  }

  /**
   * longest common sub sequence
   */
  def longestCommonSubSeq(s1: Seq[Float], s2: Seq[Float], threshold: Float): Int = {
    assert(s1.length == s2.length)
    s1.zip(s2).map(p => abs(p._1 - p._2) <= threshold).count(_ == true)
  }

  /**
   * chebyshev distance
   */
  def chebyshevDistance(s1: Seq[Float], s2: Seq[Float]): Float = {
    assert(s1.length == s2.length)
    s1.zip(s2).map(p => abs(p._1 - p._2)).max
  }

  /**
   * block distance: lower bound
   */
  def blockDistance(a: (Float, Float), b: (Float, Float)): Float = {
    val (aMax, aMin) = a
    val (bMax, bMin) = b
    Seq(
      min(abs(aMax - bMax), abs(aMax - bMin)),
      min(abs(aMin - bMax), abs(aMin - bMin)),
      min(abs(bMax - aMax), abs(bMax - aMin)),
      min(abs(bMin - aMax), abs(bMin - aMin))
    ).max
  }

  /**
   * multi block distance: lower bound
   */
  def multiBlockDistance(m1: Seq[(Float, Float)], m2: Seq[(Float, Float)]): Float = {
    assert(m1.length == m2.length)
    m1.zip(m2).map(b => blockDistance(b._1, b._2)).max
  }
}
