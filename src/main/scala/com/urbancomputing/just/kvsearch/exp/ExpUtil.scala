package com.urbancomputing.just.kvsearch.exp

import scala.io.Source

object ExpUtil {

  def readData(filePath: String): Seq[Seq[Double]] = {
    val source = Source.fromFile(filePath, "UTF-8")
    val data = source.getLines()
      .map(line => line.split("\t").last.split(",").map(_.toDouble).toSeq).toSeq
    data
  }

}
