package com.urbancomputing.just.kvsearch.util

import org.apache.hadoop.hbase.util.Bytes

object RowKeyUtils {
  // row key: [time-index, max, min, id], [Int, Float, Float, Int], where bytes are [4, 4, 4, 4]
  // todo invert the sign bite of IEEE 754 to keep the relative order of double unchanged

  /**
   * join time block index and max value to form row key prefix
   */
  def timeMaxIndex(time: Int, maxValue: Float): Array[Byte] = {
    Bytes.toBytes(time) ++ Bytes.toBytes(maxValue)
  }

  /**
   * join time block index, max value and min value to form row key prefix
   */
  def timeMaxMinIndex(time: Int, maxValue: Float, minValue: Float): Array[Byte] = {
    Bytes.toBytes(time) ++ Bytes.toBytes(maxValue) ++ Bytes.toBytes(minValue)
  }

  /**
   * join time block index, max value, min value and id to form row key
   */
  def timeMaxMinIdIndex(time: Int, maxValue: Float, minValue: Float, id: Int): Array[Byte] = {
    Bytes.toBytes(time) ++ Bytes.toBytes(maxValue) ++ Bytes.toBytes(minValue) ++ Bytes.toBytes(id)
  }

  /**
   * parse row key
   *
   * @return (time index, max, min, id)
   */
  def parseRowKey(rowKey: Array[Byte]): (Int, Float, Float, Int) = {
    (Bytes.toInt(rowKey.slice(0, 4)), Bytes.toFloat(rowKey.slice(4, 8)),
      Bytes.toFloat(rowKey.slice(8, 12)), Bytes.toInt(rowKey.slice(12, 16)))
  }

  /**
   * parse row key
   *
   * @return (time index, id)
   */
  def parseRowKeyTimeAndId(rowKey: Array[Byte]): (Int, Int) = {
    (Bytes.toInt(rowKey.slice(0, 4)), Bytes.toInt(rowKey.slice(12, 16)))
  }
}
