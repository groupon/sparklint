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

import scala.collection.mutable.ListBuffer

/**
  * @author rxue
  * @since 11/25/17.
  */
class FixedCapacityMetricsCounter(capacityMillis: Long, pruneFrequency: Long) extends MetricsCounter {
  override def push(dataPoint: WeightedInterval): Unit = {
    super.push(dataPoint)
    if (latestTime.get - earliestTime.get > capacityMillis + pruneFrequency) {
      prune()
    }
  }

  def prune(): Unit = {
    latestTime match {
      case None =>
      // no op
      case Some(l) =>
        val cutOff = l - capacityMillis
        val trimmed = ListBuffer.empty[WeightedInterval]
        var newEarliest = Long.MaxValue
        for (dp <- _dataPoints) {
          if (dp.end.exists(_ <= cutOff)) {
            // delete this records by doing nothing
          } else if (dp.start < cutOff) {
            trimmed += dp.copy(start = cutOff)
            newEarliest = cutOff
          } else {
            trimmed += dp
            newEarliest = newEarliest min dp.start
          }
        }
        _earliest = Some(newEarliest)
        _dataPoints = trimmed
    }
  }
}
