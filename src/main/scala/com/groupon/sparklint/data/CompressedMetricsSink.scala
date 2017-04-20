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

/**
  * A constant-memory-complexity data structure to store time series data points
  *
  * It produces a ganglia-like feature, that if the new data to be added is newer than the maximum this can contain,
  * a `compact` will be performed to increase the resolution (time range each bucket contains) according to `getNextResolution`,
  * so this data structure can record newer data without losing the earliest data points.
  *
  * When first created, the size of the array will be the number of buckets to keep in memory
  * The time range represented by each bucket is configured by `resolution`
  *
  * @param resolution The time range that each bucket represents, in milliseconds
  * @param dataRange  The time range that this sink represents
  * @param origin     the actual time for time0 in this data structure
  * @param storage    for each bucket, sum of millis spend in calculation
  * @author rxue
  * @since 8/11/16.
  */
class CompressedMetricsSink(override val resolution: Long,
                            override val dataRange: Option[Interval],
                            override private[sparklint] val origin: Long,
                            override private[sparklint] val storage: Array[Long]) extends MetricsSink {
  require(CompressedMetricsSink.validateResolution(resolution))

  override def addUsage(startTime: Long, endTime: Long, weight: Int = 1): CompressedMetricsSink =
    batchAddUsage(Seq(startTime -> endTime), weight)

  /*
  R => Resolution, suppose R > 10
  origin time = 20R + 5
  bucket start = 20R ([origin time] - [origin time] % [R])
  relative time = [real time] - [bucket start]

  real time    |20R 20R+5 20R-1|21R       21R-1|22R       22R-1|23R       23R-1|
  relative time|0    R+5    R-1|R           R-1|2R         2R-1|3R         3R-1|
  storage      |[    ----A----]|[------B------]|[---C---      ]|[             ]|
                ^    ^                                 ^                      ^
   BucketStart(BS)  OriginAdjusted(OA)          EndAdjusted(EA)          BucketEnd(BE)
  duration     |   BS+R-OA     |       R       |   EA%R or 0   |       0       |
  avg usage    |  A/(BS+R-OA)  |      B/R      | E/(EA%R) or 0 |       0       |
   */
  def batchAddUsage(pairs: Seq[(Long, Long)], weight: Int): CompressedMetricsSink = {
    val inputInterval = Interval(pairs.map(_._1).min, pairs.map(_._2).max)
    val desiredIndex = getBucketIndex(inputInterval.maximum)
    val (newStorage, newResolution) = if (desiredIndex >= length) {
      val desiredRatio = (desiredIndex / length) + 1
      val compactRatio = compactRatioUntil(desiredRatio)
      (compactStorage(compactRatio), resolution * compactRatio)
    } else {
      (storage.toBuffer, resolution)
    }
    pairs.foreach({ case (startTime, endTime) =>
      val startBucket = getBucketIndexWithNewResolution(startTime, newResolution)
      val endBucket = getBucketIndexWithNewResolution(endTime, newResolution)
      if (startBucket == endBucket) {
        newStorage(startBucket) += (endTime - startTime) * weight
      } else {
        newStorage(startBucket) += (newResolution - startTime % newResolution) * weight
        Range(startBucket + 1, endBucket).foreach(bucket => {
          newStorage(bucket) += newResolution * weight
        })
        newStorage(endBucket) += (endTime % newResolution) * weight
      }
    })
    val newDataRange = Some(dataRange.fold(inputInterval)(_.merge(inputInterval)))
    new CompressedMetricsSink(newResolution, newDataRange, origin, newStorage.toArray)
  }

  private[data] def getBucketIndexWithNewResolution(time: Long, resolutionToUse: Long): Int = {
    ((time - bucketStart) / resolutionToUse).toInt
  }

  /**
    * @param desiredRatio the sink should compact at least this ratio to fit new data into memory
    * @return The lowest valid ratio that is greater than desiredRatio
    */
  private[data] def compactRatioUntil(desiredRatio: Long): Long = {
    var compactRatio = 1l
    while (compactRatio < desiredRatio) {
      compactRatio = compactRatio * CompressedMetricsSink.getCompactRatio(compactRatio * resolution)
    }
    compactRatio
  }

  override def removeUsage(startTime: Long, endTime: Long, weight: Int = 1): CompressedMetricsSink =
    batchAddUsage(Seq(startTime -> endTime), -weight)

  def changeResolution(toResolution: Long): CompressedMetricsSink = {
    require(toResolution >= resolution)
    val compactRatio = toResolution / resolution
    new CompressedMetricsSink(toResolution, dataRange, origin, compactStorage(compactRatio).toArray)
  }
}

object CompressedMetricsSink {
  val validResolution = Set(1l, 5l, 20l, 100l, 500l, 1000l, 5000l, 10000l, 30000l, 60000l, 300000l, 600000l, 1800000l)

  def validateResolution(resolution: Long): Boolean = {
    validResolution.contains(resolution) || (resolution % 3600000 == 0)
  }

  def getCompactRatio(resolution: Long): Long = resolution match {
    case 1 => 5 // 1ms -> 5ms
    case 5 => 4 // 5ms -> 20ms
    case 20 => 5 // 20ms -> 100ms
    case 100 => 5 // 100ms -> 0.5s
    case 500 => 2 // 0.5s -> 1s
    case 1000 => 5 // 1s -> 5s
    case 5000 => 2 // 5s -> 10s
    case 10000 => 3 // 10s -> 30s
    case 30000 => 2 // 30s -> 1min
    case 60000 => 5 // 1min -> 5min
    case 300000 => 2 // 5min -> 10min
    case 600000 => 3 // 10min -> 30min
    case x => 2 // always double beyond 30min
  }

  def empty(originTime: Long, size: Int): CompressedMetricsSink = {
    new CompressedMetricsSink(1, None, originTime, Array.fill(size)(0L))
  }
}
