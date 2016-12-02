package com.groupon.sparklint.data

import org.apache.spark.util.StatCounter

/**
  * @author rxue
  * @since 9/23/16.
  */
class SparklintInputCounter(_bytesRead: StatCounter = StatCounter(),
                            _recordsRead: StatCounter = StatCounter()) {
  def merge(that: SparklintInputMetrics): SparklintInputCounter = {
    _bytesRead.merge(that.bytesRead)
    _recordsRead.merge(that.recordsRead)
    this
  }

  def bytesRead: StatCounter = _bytesRead.copy()

  def recordsRead: StatCounter = _recordsRead.copy()
}
