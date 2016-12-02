package com.groupon.sparklint.data

import org.apache.spark.util.StatCounter

trait SparklintTaskCounter {
  def outputMetrics          : SparklintOutputCounter

  /**
    * DataReadMethod -> SparklintInputMetrics
    * @return
    */
  def inputMetrics           : SparklintInputCounter
  def shuffleReadMetrics     : SparklintShuffleReadCounter
  def shuffleWriteMetrics    : SparklintShuffleWriteCounter
  def diskBytesSpilled       : StatCounter
  def memoryBytesSpilled     : StatCounter
  def executorDeserializeTime: StatCounter
  def jvmGCTime              : StatCounter
  def resultSerializationTime: StatCounter
  def resultSize             : StatCounter
  def executorRunTime        : StatCounter
  def merge(taskId: Long, metrics: SparklintTaskMetrics): SparklintTaskCounter
}
