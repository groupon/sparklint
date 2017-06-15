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
class LosslessMetricsSink(implicit val taskSummaryOrdering: Ordering[UsageSummary]) {
  private val dataPoints: ListBuffer[UsageSummary] = ListBuffer[UsageSummary]()

  def isEmpty: Boolean = dataPoints.isEmpty

  def nonEmpty: Boolean = dataPoints.nonEmpty

  def earliestTime: Option[Long] = dataPoints.map(_.start).reduceOption(_ min _)

  def latestTime: Option[Long] = dataPoints.map(r => r.end.getOrElse(r.start)).reduceOption(_ max _)

  def push(dataPoint: UsageSummary): Unit = {
    dataPoints += dataPoint
  }

  def sum(since: Long, until: Long): Long = {
    dataPoints.map(t => {
      ((t.end.getOrElse(until) min until) - (t.start max since)) * t.weight
    }).sum
  }

  def collect(from: Long, bucketSize: Long, until: Long): SortedMap[Long, Int] = {
    if (dataPoints.isEmpty) {
      SortedMap.empty
    } else {
      //TODO: a sorted list of datapoints can improve the complexity
      var bucketStart = from
      var buckets = SortedMap.empty[Long, Int]
      var running = dataPoints.filter(i => i.start < bucketStart && (i.end.isEmpty || i.end.get >= bucketStart))
      while (bucketStart < until) {
        val bucketEnd = bucketStart + bucketSize
        val launchedInThisBucket = dataPoints.filter(i => i.start >= bucketStart && i.start < bucketEnd)
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
        val avgUsage = if (bucketEffectiveLength == 0) 0 else (coreUsageInThisBucket.toDouble / bucketEffectiveLength).round.toInt
        buckets += (bucketStart -> avgUsage)
        bucketStart = bucketEnd
      }
      buckets += (bucketStart -> 0)
      buckets
    }
  }

}
