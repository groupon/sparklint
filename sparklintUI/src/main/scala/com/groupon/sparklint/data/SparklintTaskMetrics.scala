package com.groupon.sparklint.data

/**
  * @author rxue
  * @since 12/4/16.
  */
case class SparklintTaskMetrics(outputMetrics: SparklintOutputMetrics,
                                inputMetrics: SparklintInputMetrics,
                                shuffleReadMetrics: SparklintShuffleReadMetrics,
                                shuffleWriteMetrics: SparklintShuffleWriteMetrics,
                                diskBytesSpilled: Long,
                                memoryBytesSpilled: Long,
                                executorDeserializeTime: Long,
                                jvmGCTime: Long,
                                resultSerializationTime: Long,
                                resultSize: Long,
                                executorRunTime: Long
                               )
