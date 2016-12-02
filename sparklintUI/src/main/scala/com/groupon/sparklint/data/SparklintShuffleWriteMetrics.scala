package com.groupon.sparklint.data

/**
  * @author rxue
  * @since 9/23/16.
  */
case class SparklintShuffleWriteMetrics(shuffleBytesWritten: Long = 0L,
                                   shuffleRecordsWritten: Long = 0L,
                                   shuffleWriteTime: Long = 0L)
