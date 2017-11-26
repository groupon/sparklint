package com.groupon.sparklint.actors

/**
  * @author rxue
  * @since 11/26/17.
  */
sealed trait StorageOption {
  def emptySink: MetricsSink
}

object StorageOption {
  case object Lossless extends StorageOption {
    override def emptySink: MetricsSink = new LosslessMetricsSink
  }

  case class FixedCapacity(capacityMillis: Long, pruneFrequency: Long) extends StorageOption {
    require(capacityMillis > 0)
    require(pruneFrequency > 0)
    override def emptySink: MetricsSink = new FixedCapacityMetricsSink(capacityMillis, pruneFrequency)
  }
}
