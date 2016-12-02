package com.groupon.sparklint.data


import org.apache.spark.util.StatCounter

/**
  * @author rxue
  * @since 9/23/16.
  */
class SparklintShuffleReadCounter(_fetchWaitTime: StatCounter = StatCounter(),
                                  _localBlocksFetched: StatCounter = StatCounter(),
                                  _localBytesRead: StatCounter = StatCounter(),
                                  _recordsRead: StatCounter = StatCounter(),
                                  _remoteBlocksFetched: StatCounter = StatCounter(),
                                  _remoteBytesRead: StatCounter = StatCounter()) {
  def merge(that: SparklintShuffleReadMetrics): SparklintShuffleReadCounter = {
    _fetchWaitTime.merge(that.fetchWaitTime)
    _localBlocksFetched.merge(that.localBlocksFetched)
    _localBytesRead.merge(that.localBytesRead)
    _recordsRead.merge(that.recordsRead)
    _remoteBlocksFetched.merge(that.remoteBlocksFetched)
    _remoteBytesRead.merge(that.remoteBytesRead)
    this
  }

  def fetchWaitTime: StatCounter = _fetchWaitTime.copy()

  def localBlocksFetched: StatCounter = _localBlocksFetched.copy()

  def localBytesRead: StatCounter = _localBytesRead.copy()

  def recordsRead: StatCounter = _recordsRead.copy()

  def remoteBlocksFetched: StatCounter = _remoteBlocksFetched.copy()

  def remoteBytesRead: StatCounter = _remoteBytesRead.copy()
}
