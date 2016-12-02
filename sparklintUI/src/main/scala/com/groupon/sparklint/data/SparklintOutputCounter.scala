package com.groupon.sparklint.data

import org.apache.spark.util.StatCounter

/**
  * @author rxue
  * @since 9/23/16.
  */
class SparklintOutputCounter(_recordsWritten: StatCounter = StatCounter(),
                             _bytesWritten: StatCounter = StatCounter()) {
  def merge(that: SparklintOutputMetrics): SparklintOutputCounter = {
    _recordsWritten.merge(that.recordsWritten)
    _bytesWritten.merge(that.bytesWritten)
    this
  }

  def recordsWritten: StatCounter = _recordsWritten.copy()

  def bytesWritten: StatCounter = _bytesWritten.copy()
}

