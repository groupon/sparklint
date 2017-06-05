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


import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer
import scala.collection.{SortedMap, mutable}

/**
  * @author rxue
  * @since 6/3/17.
  */
class LosslessMetricsSink(implicit val taskSummaryOrdering: Ordering[UsageSummary]) {
  private val tasks: mutable.SortedSet[UsageSummary] = mutable.SortedSet.empty[UsageSummary]

  def isEmpty: Boolean = tasks.isEmpty

  def nonEmpty: Boolean = tasks.nonEmpty

  def earliestTime: Option[Long] = tasks.headOption.map(_.start)

  def latestTime: Option[Long] = tasks.map(r => r.end.getOrElse(r.start)).reduceOption(_ max _)

  def push(task: UsageSummary): Unit = {
    tasks += task
  }

  def sum(since: Long, until: Long): Long = {
    tasks.toSeq.map(t => {
      ((t.end.getOrElse(until) min until) - (t.start max since)) * t.weight
    }).sum
  }

  def collect(start: Long, numOfBuckets: Int, now: Long): SortedMap[Long, Int] = {
    if (tasks.isEmpty) {
      SortedMap.empty
    } else {
      val step = ((now - start).toDouble / numOfBuckets).ceil.toInt
      var current = start
      var buckets = TreeMap.empty[Long, Int]
      var runningTask = ListBuffer(tasks.until(UsageSummary(None, current)).filterNot(_.end.exists(_ < current)).toSeq: _*)
      while (current < now) {
        val bucketEnd = (current + step) min now
        // set taskId to None to make sure all tasks started at the same time is greater than this UsageSummary
        val tasksLaunchedInThisBucket = tasks.range(UsageSummary(None, current), UsageSummary(None, bucketEnd))
        runningTask ++= tasksLaunchedInThisBucket
        var coreUsageInThisBucket = 0L
        val (endsInThisBucket, stillRunning) = runningTask.partition(_.end.exists(_ < bucketEnd))
        for (taskInfo <- stillRunning) {
          coreUsageInThisBucket += (bucketEnd - (taskInfo.start max current)) * taskInfo.weight
        }
        runningTask = stillRunning
        for (taskInfo <- endsInThisBucket) {
          coreUsageInThisBucket += (taskInfo.end.get - (taskInfo.start max current)) * taskInfo.weight
        }
        val bucketEffectiveLength = (bucketEnd min now) - current
        val avgUsage = if (bucketEffectiveLength == 0) 0 else (coreUsageInThisBucket.toDouble / bucketEffectiveLength).round.toInt
        buckets += (current -> avgUsage)
        current = bucketEnd
      }
      buckets += (current -> 0)
      buckets
    }
  }

}
