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

package com.groupon.sparklint.actors


import scala.collection.SortedMap
import scala.collection.mutable.ListBuffer

/**
  * @author rxue
  * @since 6/3/17.
  */
trait MetricsSink {
  protected var _dataPoints: ListBuffer[WeightedInterval] = ListBuffer[WeightedInterval]()

  protected var _earliest: Option[Long] = None
  protected var _latest: Option[Long] = None

  def isEmpty: Boolean = _dataPoints.isEmpty

  def nonEmpty: Boolean = _dataPoints.nonEmpty

  def earliestTime: Option[Long] = _earliest

  def latestTime: Option[Long] = _latest

  def push(dataPoint: WeightedInterval): Unit = {
    _dataPoints += dataPoint
    if (_earliest.isEmpty || _earliest.exists(_ > dataPoint.start)) {
      _earliest = Some(dataPoint.start)
    }
    if (_latest.isEmpty || _latest.exists(_ < dataPoint.end.getOrElse(dataPoint.start))) {
      _latest = Some(dataPoint.end.getOrElse(dataPoint.start))
    }
  }

  def sum(from: Long, until: Long): Long = {
    _dataPoints.map(t => {
      ((t.end.getOrElse(until) min until) - (t.start max from)) * t.weight
    }).sum
  }

  /**
    * Collect stats
    * @param from start counting from this time
    * @param until count until this time
    * @param bucketSize group stats into time buckets of this size (millis)
    * @return SortedMap of [bucketBegin, averageNumberOfDataPointsInBucket]
    */
  def collect(from: Long, until: Long, bucketSize: Long): SortedMap[Long, Double] = {
    if (_dataPoints.isEmpty) {
      SortedMap.empty
    } else {
      //TODO: a sorted list of datapoints can improve the complexity
      var bucketStart = from
      var buckets = SortedMap.empty[Long, Double]
      var running = _dataPoints.filter(i => i.start < bucketStart && (i.end.isEmpty || i.end.get >= bucketStart))
      while (bucketStart < until) {
        val bucketEnd = (bucketStart - bucketStart % bucketSize + bucketSize) min until
        val launchedInThisBucket = _dataPoints.filter(i => i.start >= bucketStart && i.start < bucketEnd)
        running ++= launchedInThisBucket
        var coreUsageInThisBucket = 0L
        val (endsInThisBucket, stillRunning) = running.partition(_.end.exists(_ < bucketEnd))
        for (dataPoint <- stillRunning) {
          coreUsageInThisBucket += (bucketEnd - (dataPoint.start max bucketStart)) * dataPoint.weight
        }
        running = stillRunning
        for (dataPoint <- endsInThisBucket) {
          coreUsageInThisBucket += (dataPoint.end.get - (dataPoint.start max bucketStart)) * dataPoint.weight
        }
        val bucketEffectiveLength = (bucketEnd min until) - bucketStart
        val avgUsage = if (bucketEffectiveLength == 0) 0.0 else coreUsageInThisBucket.toDouble / bucketEffectiveLength
        buckets += (bucketStart -> avgUsage)
        bucketStart = bucketEnd
      }
      buckets += bucketStart -> 0.0
      buckets
    }
  }

}


