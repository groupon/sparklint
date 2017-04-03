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
  *
  * @param dataRange       The time range that this sink represents
  * @param numBuckets      When output, the numberOfBucket
  * @param origin          the actual time for time0 in this data structure
  * @param losslessStorage the storage data strucutre
  * @author rxue
  * @since 8/16/16.
  */
class LosslessMetricsSink(val dataRange: Option[Interval],
                          val numBuckets: Int,
                          private[sparklint] val origin: Long,
                          private[sparklint] val losslessStorage: Map[Interval, Int]) extends MetricsSink {


  override lazy val resolution: Long = {
    var _resolution = 1L
    if (dataRange.nonEmpty) {
      val desiredRange = dataRange.get.merge(Interval(origin, origin)).length
      if (desiredRange > 0) {
        while ((desiredRange / _resolution + 1) >= numBuckets) {
          _resolution *= CompressedMetricsSink.getCompactRatio(_resolution)
        }
      }
    }
    _resolution
  }

  override private[sparklint] lazy val storage: Array[Long] = {
    val _storage = ArrayBuffer.fill(numBuckets)(0L)
    losslessStorage.foreach({ case (interval, weight) =>
      val startBucket = getBucketIndexWithNewResolution(interval.minimum, resolution)
      val endBucket = getBucketIndexWithNewResolution(interval.maximum, resolution)
      if (startBucket == endBucket) {
        _storage(startBucket) += interval.length * weight
      } else {
        _storage(startBucket) += (resolution - interval.minimum % resolution) * weight
        Range(startBucket + 1, endBucket).foreach(bucket => {
          _storage(bucket) += resolution * weight
        })
        _storage(endBucket) += (interval.maximum % resolution) * weight
      }
    })
    _storage.toArray
  }

  override def changeResolution(toResolution: Long): MetricsSink = {
    val numBucketsNeeded = if (dataRange.nonEmpty) {
      val desiredRange = dataRange.get.merge(Interval(origin, origin)).length
      (desiredRange / toResolution + 1).toInt
    } else {
      1
    }
    new LosslessMetricsSink(dataRange, numBucketsNeeded, origin, losslessStorage)
  }

  override def addUsage(startTime: Long, endTime: Long, weight: Int = 1): LosslessMetricsSink =
    batchAddUsage(Seq(startTime -> endTime), weight)

  override def batchAddUsage(pairs: Seq[(Long, Long)], weight: Int): LosslessMetricsSink = {
    val inputInterval = Interval(pairs.map(_._1).min, pairs.map(_._2).max)
    val newDataRange = Some(dataRange.fold(inputInterval)(_.merge(inputInterval)))
    val newStorage = mutable.Map(losslessStorage.toSeq: _*)
    pairs.foreach({ case (startTime, endTime) =>
      val interval = Interval(startTime, endTime)
      newStorage.get(interval) match {
        case None => newStorage(interval) = weight
        case Some(x) if x + weight == 0 => newStorage.remove(interval)
        case Some(x) => newStorage(interval) = x + weight
      }
    })
    new LosslessMetricsSink(newDataRange, numBuckets, origin, newStorage.toMap)
  }

  override def removeUsage(startTime: Long, endTime: Long, weight: Int = 1): LosslessMetricsSink =
    batchAddUsage(Seq(startTime -> endTime), -weight)

  private[data] def getBucketIndexWithNewResolution(time: Long, resolutionToUse: Long): Int = {
    ((time - bucketStart) / resolutionToUse).toInt
  }
}

object LosslessMetricsSink {
  def empty(originTime: Long, numBuckets: Int): LosslessMetricsSink = new LosslessMetricsSink(None, numBuckets, originTime, Map.empty)
}
