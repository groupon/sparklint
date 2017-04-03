/*
 * Copyright 2016 Groupon, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.groupon.sparklint.data

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author rxue
  * @since 9/23/16.
  */
trait MetricsSink {
  lazy val length: Int = storage.length
  lazy val bucketStart: Long = origin - origin % resolution
  lazy val bucketEnd: Long = bucketStart + resolution * length
  val resolution: Long
  val dataRange: Option[Interval]
  private[sparklint] val origin: Long
  private[sparklint] val storage: Array[Long]

  def apply(index: Int): Long = storage(index)

  def getAvgValueForTime(time: Long): Option[Double] = {
    getValueForTime(time).map(_.toDouble / resolution)
  }

  def getValueForTime(time: Long): Option[Long] = {
    getBucket(getBucketIndex(time))
  }

  def getBucket(index: Int): Option[Long] = {
    if (index < 0 || index >= length) {
      None
    } else {
      Some(storage(index))
    }
  }

  def getBucketIndex(time: Long): Int = {
    ((time - bucketStart) / resolution).toInt
  }

  def nonEmpty: Boolean = dataRange.nonEmpty

  def changeResolution(toResolution: Long): MetricsSink

  def batchAddUsage(pairs: Seq[(Long, Long)], weight: Int): MetricsSink

  def addUsage(startTime: Long, endTime: Long, weight: Int = 1): MetricsSink

  def removeUsage(startTime: Long, endTime: Long, weight: Int = 1): MetricsSink

  /**
    * Get usage distribution
    *
    * @return
    */
  def convertToUsageDistribution: Map[Int, Long] = {
    dataRange match {
      case Some(interval) =>
        val startIndex = getBucketIndex(interval.minimum)
        val endIndex = getBucketIndex(interval.maximum)
        val bucketsToUse = storage.slice(startIndex, endIndex + 1)
        if (bucketsToUse.length == 1) {
          Map((bucketsToUse.head.toDouble / interval.length).round.toInt -> interval.length)
        } else {
          bucketsToUse.zipWithIndex.map({ case (value, index) =>
            val duration = if (index == startIndex) {
              resolution - interval.minimum % resolution
            } else if (index == endIndex) {
              interval.maximum % resolution + 1
            } else {
              resolution
            }
            (value.toDouble / duration).round.toInt -> duration
          }).groupBy(_._1).mapValues(_.map(_._2).sum)
        }
      case None =>
        Map.empty
    }
  }

  private[data] def compactStorage(compactRatio: Long): mutable.Buffer[Long] = {
    val offset = ((bucketStart / resolution) % compactRatio).toInt
    val newStorage = ArrayBuffer.fill(length)(0L)
    var newIndex = 0
    Range(0, length).foreach(index => {
      if (index != 0 && (index + offset) % compactRatio == 0) {
        newIndex += 1
      }
      newStorage(newIndex) += storage(index)
    })
    newStorage
  }
}

object MetricsSink {
  /**
    * for multiple metrics sinks, first align their resolution to the same level, then align their start time
    * so we can have a unified metrics sink covering the majority information of every sinks
    *
    * @param sinks the metrics sinks to merge
    * @return merged metrics sink
    */
  def mergeSinks(sinks: Iterable[MetricsSink]): MetricsSink = {
    require(sinks.nonEmpty)
    /* Align resolutions
    *    [A][B][C]        => [ A ][B C]
    * [  D ][ E  ]        => [ D ][ E ]
    *       [F][G][H][I]  =>      [F G][H I]
    */
    val maxResolution = sinks.map(_.resolution).max
    val sinksWithSameResolution = sinks.map(sink => {
      if (sink.resolution < maxResolution) {
        sink.changeResolution(maxResolution)
      } else {
        sink
      }
    })
    /* Align start time and merge
    * [ A ][ B ]
    * [ C ][ D ]
    *      [ E ][ F ]
    * ---------------
    * [A C][BDE][ F ]
     */
    val dataRanges = sinks.flatMap(_.dataRange)
    val dataInterval = if (dataRanges.isEmpty) None else Some(dataRanges.reduce(_ merge _))
    val origin = sinks.map(_.origin).min
    val bucketStart = sinksWithSameResolution.map(_.bucketStart).min
    val bucketEnd = sinksWithSameResolution.map(_.bucketEnd).max
    val mergedStorage = Array.fill(((bucketEnd - bucketStart) / maxResolution).toInt)(0L).toBuffer
    sinksWithSameResolution.foreach(sink => {
      val firstBucketIndex = ((sink.bucketStart - bucketStart) / maxResolution).toInt
      Range(0, sink.length).foreach(index => {
        mergedStorage(firstBucketIndex + index) += sink(index)
      })
    })
    new CompressedMetricsSink(maxResolution, dataInterval, origin, mergedStorage.toArray)
  }
}
