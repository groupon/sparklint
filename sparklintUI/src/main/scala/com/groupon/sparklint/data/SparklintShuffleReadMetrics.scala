package com.groupon.sparklint.data

/**
  * @author rxue
  * @since 9/23/16.
  */
case class SparklintShuffleReadMetrics(fetchWaitTime: Long = 0L,
                                  localBlocksFetched: Long = 0L,
                                  localBytesRead: Long = 0L,
                                  recordsRead: Long = 0L,
                                  remoteBlocksFetched: Long = 0L,
                                  remoteBytesRead: Long = 0L)
