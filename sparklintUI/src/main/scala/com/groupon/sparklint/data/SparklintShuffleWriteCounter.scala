package com.groupon.sparklint.data

import org.apache.spark.util.StatCounter

/**
  * @author rxue
  * @since 9/23/16.
  */
class SparklintShuffleWriteCounter(_shuffleBytesWritten: StatCounter = StatCounter(),
                                   _shuffleRecordsWritten: StatCounter = StatCounter(),
                                   _shuffleWriteTime: StatCounter = StatCounter()) {
  def merge(that: SparklintShuffleWriteMetrics): SparklintShuffleWriteCounter = {
    _shuffleBytesWritten.merge(that.shuffleBytesWritten)
    _shuffleRecordsWritten.merge(that.shuffleRecordsWritten)
    _shuffleWriteTime.merge(that.shuffleWriteTime)
    this
  }

  def shuffleBytesWritten: StatCounter = _shuffleBytesWritten.copy()

  def shuffleRecordsWritten: StatCounter = _shuffleRecordsWritten.copy()

  def shuffleWriteTime: StatCounter = _shuffleWriteTime.copy()
}
