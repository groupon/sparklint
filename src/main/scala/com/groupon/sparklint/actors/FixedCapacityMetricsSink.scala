package com.groupon.sparklint.actors

import scala.collection.mutable.ListBuffer

/**
  * @author rxue
  * @since 11/25/17.
  */
class FixedCapacityMetricsSink(capacityMillis: Long, pruneFrequency: Long) extends MetricsSink {
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
